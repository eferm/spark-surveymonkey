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
import itertools
import functools

import jq
from pyspark.sql import functions as F
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


def _create_map(details_json, base_string, key_string, val_string):
    """construct pyspark map from jq strings"""
    keys = jq.compile(base_string + key_string).input(details_json).all()
    vals = jq.compile(base_string + val_string).input(details_json).all()
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


def _flatten(spark_session, path_to_json_files):
    """take a collection of JSON files and convert to DataFrame
    supports JSON format from SurveyMonkey API endpoint
    v3/surveys/{survey_id}/responses
    """
    df = spark_session.read.json(str(path_to_json_files))
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
    for c in ['col_id', 'other_id']:
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


def _interpret(df, details):
    """take basic flattened DataFrame and translate _ids to human readable format
    assumes details_json is output from SurveyMonkey API endpoint
    v3/surveys/{survey_id}/details
    """
    map_ = functools.partial(_create_map, details)
    base = '.pages[].questions[]'
    jqpath = {
        'questions': base,
        'choices': f'{base}.answers | select(. != null) | .choices | select(. != null) | .[]',
        'rows': f'{base}.answers | select(. != null) | .rows | select(. != null) | .[]',
        'cols': f'{base}.answers | select(. != null) | .cols | select(. != null) | .[].choices[]',
    }

    df = (
        df
        # look up _ids
        .withColumn('family', map_(jqpath['questions'], '.id', '.family')[F.col('question_id')])
        .withColumn('heading', map_(
            jqpath['questions'], '.id', '.headings[0].heading')[F.col('question_id')])
        .withColumn('heading', F.regexp_replace('heading', r'<[^>]*>', ''))  # strip HTML tags
        .withColumn('row', map_(jqpath['rows'], '.id', '.text')[F.col('row_id')])
        .withColumn('choice', F.coalesce(
            map_(jqpath['choices'], '.id', '.text')[F.col('choice_id')],
            map_(jqpath['cols'], '.id', '.text')[F.col('choice_id')]))

        # flag for other
        .withColumn('other', map_(
            jqpath['questions'], '.id', '.answers.other.is_answer_choice')[F.col('question_id')])

        # construct column names for later pivoting
        .withColumn(
            'column',
            # demographic
            F.when(F.col('family') == 'demographic', F.concat_ws('_', norm('heading'), norm('row')))

             # single_choice
             .when((F.col('family') == 'single_choice')
                   & F.col('other') & F.col('other_id').isNotNull(),
                   F.concat_ws('_', norm('heading'), F.lit('other')))
             .when(F.col('family') == 'single_choice', norm('heading'))

             # open_ended
             .when(F.col('family') == 'open_ended', F.concat_ws('_', norm('heading'), norm('row')))

             # multiple_choice
             .when((F.col('family') == 'multiple_choice')
                   & F.col('other') & F.col('other_id').isNotNull(),
                   F.concat_ws('_', norm('heading'), F.lit('other')))
             .when(F.col('family') == 'multiple_choice',
                   F.concat_ws('_', norm('heading'), norm('choice')))

             # matrix
             .when(F.col('family') == 'matrix', F.concat_ws('_', norm('heading'), norm('row')))

             # catch-all
             .otherwise(F.concat_ws('_', F.lit('unparsed_question'), F.col('question_id')))
        )
        # pick field that constitutes "response"
        .withColumn(
            'value',
            F.when((F.col('family') == 'demographic'), F.col('text'))
             .when((F.col('family') == 'single_choice') & F.col('other_id').isNotNull(),
                   F.col('text'))
             .when((F.col('family') == 'single_choice'), F.col('choice'))
             .when((F.col('family') == 'open_ended'), F.col('text'))
             .when((F.col('family') == 'multiple_choice') & F.col('other_id').isNotNull(),
                   F.col('text'))
             .when((F.col('family') == 'multiple_choice'), F.col('choice'))
             .when((F.col('family') == 'matrix'), F.col('choice'))
        )
    )
    return df


def _pivot(df):
    """convert to wide format"""

    # dedup questions with same column name
    df = (
        df
        .withColumn('_rank', F.dense_rank().over(
            Window.partitionBy('column').orderBy('question_id')))
        .withColumn('column', F.concat_ws('_', 'column', '_rank'))
    )
    
    # pivot to wide format
    df = (
        df
        # include _id in col names to allow for column ordering
        .withColumn('column', F.concat_ws('_', F.lit('_'), 'question_id', 'row_id', 'column'))
        .groupBy(RESPONSE_KEY)
        .pivot('column')
        .agg(F.first('value'))
    )

    # set column order
    question_cols = set(df.columns) - set(RESPONSE_KEY)
    columns = RESPONSE_KEY + sorted(question_cols)
    df = df.select(*columns)

    # figure out single choice questions with "Other" option
    questions_w_other = []
    base = columns[0]
    for col in columns:
        # don't consider enumerator
        b = re.sub(r'_\d+$', '', base)
        # if column looks like `this_is_the_stem_other`
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
    renamed = map(lambda s: re.sub(r'^__[\d+_]+', '', s), df.columns)
    renamed = map(lambda s: re.sub(r'_1$', '', s), renamed)
    df = df.toDF(*renamed)
    return df


def transform_surveymonkey(spark_session, path_to_json_files, details):
    df = _flatten(spark_session, path_to_json_files)
    df = _interpret(df, details)
    df = _pivot(df)
    return df
