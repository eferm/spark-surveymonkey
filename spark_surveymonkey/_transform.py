"""MIT License

Copyright (c) 2020 Emanuel Ferm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import re
import json
import itertools
import functools
from typing import Union, Dict

import jq
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window


def blank_as_null(x):
    return F.when(F.col(x) != '', F.col(x)).otherwise(None)


def norm(col, maxlen=50):
    """make a string column-name friendly"""
    col = F.col(col)
    col = F.trim(col)
    col = F.lower(col)
    col = F.regexp_replace(col, r'[^a-z0-9_]', '_')
    col = F.regexp_replace(col, r'_{2,}', '_')
    if maxlen:
        col = col.substr(1, maxlen)
    col = F.regexp_replace(col, r'^_', '')
    col = F.regexp_replace(col, r'_$', '')
    return col


def map_from_json(details, node, key, val):
    """construct pyspark map from jq strings"""
    keys = jq.compile(node + key).input(details).all()
    vals = jq.compile(node + val).input(details).all()
    flattened = itertools.chain.from_iterable(zip(keys, vals))
    return F.create_map(*map(F.lit, flattened))


RESPONSE_KEY = [
    'response_id',
    'recipient_id',
    'collection_mode',
    'response_status',
    'custom_value',
    'first_name',
    'last_name',
    'email_address',
    'ip_address',
    'metadata',
    'page_path',
    'collector_id',
    'survey_id',
    'edit_url',
    'analyze_url',
    'total_time',
    'date_modified',
    'date_created',
    'href',
]


def flatten(spark_session, survey_responses):
    """take a collection of JSON files and convert to DataFrame
    supports JSON format from SurveyMonkey API endpoint
    v3/surveys/{survey_id}/responses
    """
    df = spark_session.read.json(str(survey_responses))
    df = (
        df
        # expand responses
        .select(F.explode('data').alias('data'))
        .select('data.*')
        .withColumnRenamed('id', 'response_id')

        # expand pages
        .withColumn('pages', F.explode('pages'))
        .select('*', 'pages.*')
        .withColumnRenamed('id', 'page_id')

        # expand questions
        .withColumn('questions', F.explode('questions'))
        .select('*', 'questions.*')
        .withColumnRenamed('id', 'question_id')

        # expand answers
        .withColumn('answers', F.explode('answers'))
        .select('*', 'answers.*')
    )

    for col, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(col, blank_as_null(col))

    # columns that sometimes don't exist
    for c in ['choice_id', 'row_id', 'col_id', 'other_id']:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast('long'))

    for c in RESPONSE_KEY:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast('string'))

    return df.select(
        *RESPONSE_KEY,
        'page_id',
        'question_id',
        'choice_id',
        'row_id',
        'col_id',
        'other_id',
        'tag_data',
        'text'
    )


def interpret(df, survey_details):
    """take basic flattened DataFrame and translate _ids to human readable format
    assumes details_json is output from SurveyMonkey API endpoint
    v3/surveys/{survey_id}/details
    """
    if isinstance(survey_details, str):
        with open(survey_details) as f:
            survey_details = json.load(f)

    jqmap = functools.partial(map_from_json, survey_details)
    root = '.pages[].questions[]'
    node = {
        'page': '.pages[]',
        'question': '.pages[].questions[]',
        'choice': '.pages[].questions[].answers | select(. != null) | .choices | select(. != null) | .[]',
        'row': '.pages[].questions[].answers | select(. != null) | .rows | select(. != null) | .[]',
        'col': '.pages[].questions[].answers | select(. != null) | .cols | select(. != null) | .[].choices[]',
    }

    df = (
        df
        .withColumn('page_idx', F.lpad(jqmap(node['page'], '.id', '.position')[F.col('page_id')], 5, '0'))
        .withColumn('question_idx', F.lpad(jqmap(node['question'], '.id', '.position')[F.col('question_id')], 5, '0'))
        .withColumn('family', jqmap(node['question'], '.id', '.family')[F.col('question_id')])

        .withColumn('heading', jqmap(node['question'], '.id', '.headings[0].heading')[F.col('question_id')])
        .withColumn('heading', F.regexp_replace('heading', r'<[^>]*>', ''))  # strip HTML tags
        .withColumn('heading_norm', norm('heading'))

        .withColumn('row', jqmap(node['row'], '.id', '.text')[F.col('row_id')])
        .withColumn('row', F.regexp_extract('row', r'^\s*(.*)\s*$', 1))
        .withColumn('row', F.regexp_replace('row', r'<[^>]*>', ''))  # strip HTML tags
        .withColumn('row_norm', norm('row'))

        .withColumn('choice', F.coalesce(
            jqmap(node['choice'], '.id', '.text')[F.col('choice_id')],
            jqmap(node['col'], '.id', '.text')[F.col('choice_id')])
        )
        # construct column names for later pivoting
        .withColumn(
            'column',
            F.when(F.col('other_id').isNotNull(), F.concat_ws('_', 'heading_norm', F.lit('other')))
             .when(F.col('family') == 'demographic', F.concat_ws('_', 'heading_norm', 'row_norm'))
             .when(F.col('family') == 'single_choice', F.col('heading_norm'))
             .when(F.col('family') == 'open_ended', F.concat_ws('_', 'heading_norm', 'row_norm'))
             .when(F.col('family') == 'multiple_choice', F.concat_ws('_', 'heading_norm', norm('choice')))
             .when(F.col('family') == 'matrix', F.concat_ws('_', 'heading_norm', 'row_norm'))
             .when(F.col('family') == 'datetime', F.concat_ws('_', 'heading_norm', 'row_norm'))
             .otherwise(F.concat_ws('_', F.lit('unparsed_question'), 'question_id'))
        )
        # pick field that constitutes "response"
        .withColumn(
            'value',
            F.when(F.col('other_id').isNotNull(), F.col('text'))
             .when(F.col('family') == 'demographic', F.col('text'))
             .when(F.col('family') == 'single_choice', F.col('choice'))
             .when(F.col('family') == 'open_ended', F.col('text'))
             .when(F.col('family') == 'multiple_choice', F.col('choice'))
             .when(F.col('family') == 'matrix', F.col('choice'))
             .when(F.col('family') == 'datetime', F.col('text'))
        )
    )
    return df


def pivot(df):
    """convert to wide format"""
    df = (
        df
        # dedup questions with same column name
        .withColumn('rank', F.dense_rank().over(Window.partitionBy('column').orderBy('question_id')))
        # deterministic ordering for questions
        .withColumn('order_by', F.concat_ws('_', 'page_idx', 'question_idx'))
        # order within a question
        .withColumn(
            'order_by',
            F.when(F.col('family') != 'single_choice',
                   F.concat_ws('_', 'order_by', F.coalesce('choice_id', 'row_id', 'other_id'))
            ).otherwise(F.col('order_by'))
        )
        # construct sortable column names
        .withColumn('column', F.concat_ws('_', F.lit('_'), 'order_by', 'column', 'rank'))
        .groupBy(RESPONSE_KEY)
        .pivot('column')
        .agg(F.first('value'))
    )

    # set column order
    question_cols = set(df.columns) - set(RESPONSE_KEY)
    columns = RESPONSE_KEY + sorted(question_cols)
    df = df.select(*columns)

    # find single choice questions with "Other" option
    questions_w_other = []
    base = columns[0]
    for col in columns:
        b = re.sub(r'_\d+$', '', base)  # don't consider enumerator
        # if column looks like `this_is_the_base_other`
        if b in col and 'other' in col:
            questions_w_other.append((base, col))
        base = col

    # inject "Other" for single choice questions
    for base, other in questions_w_other:
        df = df.withColumn(
            base,
            F.when(
                F.col(other).isNotNull(),
                F.coalesce(F.col(base), F.lit('Other (please specify)'))
            ).otherwise(F.col(base))
        )

    # drop __question_id prefixes and _1 suffixes
    names = df.columns
    names = map(lambda s: re.sub(r'^__[\d+_]+', '', s), names)
    names = map(lambda s: re.sub(r'_1$', '', s), names)
    df = df.toDF(*names)
    return df


def transform_survey(spark_session: SparkSession,
                     survey_responses: str,
                     survey_details: Union[str, Dict[str, str]]) -> DataFrame:
    """Convert SurveyMonkey responses in JSON to PySpark `DataFrame`.

    Args:
        spark_session: A `SparkSession` object.
        survey_responses: Path to directory containing JSON files.
            Files retrieved from API endpoint `surveys/{survey_id}/responses/bulk`.
        survey_details: Path to JSON file, or `dict` of JSON file.
            File retrieved from API endpoint `surveys/{survey_id}/details`.

    Returns:
        `DataFrame`: Survey responses with 1 row per response.
    """
    df = flatten(spark_session, survey_responses)
    df = interpret(df, survey_details)
    df = pivot(df)
    return df
