import argparse
import sys
import subprocess
import time
import threading
import tempfile
import os
import shutil
import re


class ErrorContainer:
    def __init__(self):
        self._lock = threading.Lock()
        self._errors = []

    def append(self, item):
        with self._lock:
            self._errors.append(item)

    def get_errors(self):
        with self._lock:
            return list(self._errors)

    def __len__(self):
        with self._lock:
            return len(self._errors)


error_container = ErrorContainer()


def valid_timeout(value):
    try:
        timeout_float = float(value)
        if timeout_float <= 0:
            raise argparse.ArgumentTypeError("Timeout value must be a positive float")
        return timeout_float
    except ValueError:
        raise argparse.ArgumentTypeError("Timeout value must be a float")


lines = [
    "test/optimizer/pushdown/issue_16104.test",
    "test/fuzzer/pedro/nan_as_seed.test",
    "test/sql/function/numeric/test_random.test",
    "test/sql/function/uuid/test_uuid.test",
    "test/sql/window/test_volatile_independence.test",
    "test/fuzzer/pedro/having_query_wrong_result.test",
    "test/fuzzer/pedro/temp_sequence_durability.test",
    "test/issues/fuzz/sequence_overflow.test",
    "test/sql/aggregate/aggregates/test_bit_xor.test",
    "test/sql/aggregate/aggregates/test_bit_and.test",
    "test/sql/aggregate/aggregates/test_bit_or.test",
    "test/sql/aggregate/aggregates/test_avg.test",
    "test/sql/catalog/comment_on_wal.test",
    "test/sql/catalog/dependencies/test_alter_dependency_ownership.test",
    "test/sql/catalog/function/test_sequence_macro.test",
    "test/sql/catalog/sequence/sequence_offset_increment.test",
    "test/sql/catalog/sequence/sequence_cycle.test",
    "test/sql/catalog/sequence/test_sequence.test",
    "test/sql/catalog/sequence/sequence_overflow.test",
    "test/sql/function/list/aggregates/avg.test",
    "test/sql/function/list/aggregates/bit_and.test",
    "test/sql/function/list/aggregates/bit_xor.test",
    "test/sql/function/list/aggregates/bit_or.test",
    "test/sql/storage/wal/wal_store_sequences.test",
    "test/sql/storage/wal/wal_store_default_sequence.test",
    "test/sql/storage/wal/wal_sequence_uncommitted_transaction.test",
    "test/sql/storage/catalog/test_sequence_uncommitted_transaction.test",
    "test/sql/storage/catalog/test_store_default_sequence.test",
    "test/sql/storage/catalog/test_store_sequences.test",
    "test/sql/attach/reattach_schema.test",
    "test/sql/attach/attach_sequence.test",
    "test/sql/export/export_database.test",
    "test/sql/copy_database/copy_database_different_types.test",
    "test/sql/catalog/test_temporary.test",
    "test/sql/catalog/sequence/test_duckdb_sequences.test",
    "test/fuzzer/pedro/sample_limit_overflow.test",
    "test/sql/function/numeric/set_seed_for_sample.test",
    "test/sql/parser/test_value_functions.test",
    "test/sql/function/timestamp/current_time.test",
    "test/sql/timezone/test_icu_timezone.test"
    "test/sql/parallelism/intraquery/test_parallel_nested_aggregates.test"
    "test/sql/subquery/scalar/test_issue_6136.test"
    "test/sql/pragma/test_query_log.test",
    "test/sql/copy/csv/rejects/csv_rejects_auto.test",
    "test/sql/copy/csv/rejects/csv_rejects_flush_cast.test",
    "test/sql/copy/csv/rejects/csv_unquoted_rejects.test",
    "test/sql/copy/csv/rejects/csv_rejects_read.test",
    "test/sql/copy/csv/rejects/csv_rejects_maximum_line.test",
    "test/sql/copy/csv/rejects/test_invalid_utf_rejects.test",
    "test/sql/copy/csv/rejects/csv_rejects_flush_message.test",
    "test/sql/copy/csv/rejects/csv_rejects_two_tables.test",
    "test/sql/copy/csv/rejects/test_mixed.test",
    "test/sql/copy/csv/rejects/test_multiple_errors_same_line.test",
    "test/sql/copy/csv/rejects/csv_incorrect_columns_amount_rejects.test",
    "test/sql/copy/csv/test_non_unicode_header.test",
    "test/sql/logging/test_logging_function.test",
    "test/sql/logging/logging_types.test",
    "test/sql/logging/logging.test",
    "test/sql/logging/file_system_logging.test",
    "test/fuzzer/pedro/strptime_null_argument.test",
    "test/fuzzer/pedro/force_no_cross_product.test",
    "test/fuzzer/sqlsmith/bitstring_agg_overflow.test",
    "test/fuzzer/duckfuzz/semi_join_has_correct_left_right_relations.test",
    "test/issues/rigger/overflow_filter_pushdown.test",
    "test/sql/optimizer/test_in_rewrite_rule.test",
    "test/sql/aggregate/aggregates/test_bitstring_agg.test",
    "test/sql/storage/compression/rle/rle_constant.test",
    "test/sql/copy/parquet/parquet_filename_filter.test",
    "test/sql/copy/parquet/union_by_name_hive_partitioning.test",
    "test/sql/copy/parquet/parquet_hive.test",
    "test/optimizer/statistics/statistics_numeric.test",
    "test/fuzzer/pedro/vacuum_table_with_generated_column.test",
    "test/sql/types/struct/struct_stats.test",
    "test/sql/types/list/list_stats.test",
    "test/sql/types/nested/array/array_statistics.test",
    "test/sql/vacuum/test_analyze.test",
    "test/sql/function/generic/test_stats.test",
    "test/sql/storage/distinct_statistics_storage.test",
    "test/sql/pg_catalog/pg_prepared_statements.test",
    "test/sql/alter/add_col/test_add_col_stats.test",
    "test/sql/table_function/duckdb_prepared_statements.test"
    "test/sql/function/list/lambdas/arrow/test_deprecated_lambda.test"
    "test/optimizer/joins/no_duplicate_elimination_join.test",
    "test/sql/optimizer/plan/test_filter_pushdown_large.test",
    "test/sql/limit/test_limit0.test" "test/sql/limit/test_limit0.test",
    "test/sql/sample/reservoir_testing_percentage.test",
    "test/sql/copy/csv/test_15211.test",
    "test/sql/types/decimal/test_decimal_from_string.test",
    "test/issues/general/test_15483.test",
    "test/sql/storage/compression/dict_fsst/dict_fsst_test.test",
    "test/sql/window/test_thread_count.test",
    "test/sql/storage/lazy_load/lazy_load_limit.test",
    "test/sql/types/nested/map/test_map_concat.test",
    "test/sql/storage/partial_blocks/many_columns_drop_table.test",
    "test/sql/function/timestamp/test_now_prepared.test",
    "test/sql/copy/csv/csv_line_too_long.test",
    "test/sql/cte/recursive_cte_key_variant.test",
    "test/sql/error/test_try_expression.test",
    "test/sql/function/generic/least_greatest_types.test",
    "test/sql/storage/partial_blocks/many_columns_strings.test",
    "test/sql/storage/partial_blocks/many_columns_storage.test",
    "test/sql/storage/partial_blocks/many_columns_validity.test",
    "test/sql/copy/parquet/parquet_hive2.test",
    "test/extension/install_extension.test",
    "test/sql/storage/partial_blocks/many_columns_rle.test",
    "test/sql/storage/test_store_integers.test",
    "test/sql/window/test_window_range.test",
    "test/sql/window/test_window_groups.test",
    "test/sql/copy/partitioned/hive_partitioned_auto_detect.test",
    "test/sql/copy/csv/code_cov/csv_disk_reload.test",
    "test/sql/window/test_window_constant_aggregate.test",
    "test/sql/pg_catalog/sqlalchemy.test",
    "test/sql/copy/csv/test_non_unicode_header.test",
    "test/sql/copy/csv/auto/test_describe_order.test",
    "test/sql/storage/compression/bitpacking/bitpacking_constant_delta.test",
    "test/sql/secrets/create_secret_persistence_no_client_context.test",
    "test/sql/copy/csv/null_padding_big.test",
    "test/sql/copy/csv/test_sniff_csv.test",
    "test/sql/extensions/allowed_directories_install.test",
    "test/sql/sample/bernoulli_sampling.test",
    "test/sql/copy/csv/test_copy.test",
    "test/sql/copy/csv/test_thijs_unquoted_file.test",
    "test/sql/function/date/test_date_part.test",
    "test/sql/copy/parquet/test_parallel_many_row_groups.test",
    "test/sql/copy/parquet/parquet_stats.test",
    "test/optimizer/pushdown/filter_cannot_pushdown.test",
    "test/sql/copy/csv/test_comment_option.test",
    "test/sql/sample/same_seed_same_sample.test",
    "test/sql/copy/parquet/parquet_write_codecs.test",
    "test/sql/function/list/aggregates/incorrect.test",
    "test/sql/copy/csv/csv_hive.test",
    "test/sql/types/struct/struct_comparison.test",
    "test/sql/types/list/list_comparison.test",
    "test/sql/aggregate/distinct/grouped/identical_inputs.test",
    "test/sql/window/test_empty_frames.test",
    "test/sql/index/art/nodes/test_art_node_256.test",
    "test/sql/aggregate/aggregates/test_any_value.test",
    "test/sql/aggregate/aggregates/test_last.test",
    "test/sql/aggregate/distinct/ungrouped/test_distinct_ungrouped.test",
    "test/sql/pg_catalog/pg_type.test",
    "test/sql/index/art/storage/test_art_duckdb_versions.test",
    "test/sql/optimizer/expression/test_move_constants.test",
    "test/sql/function/list/aggregates/null_or_empty.test",
    "test/sql/function/list/list_zip.test",
    "test/sql/function/list/list_concat.test",
    "test/sql/function/timestamp/test_date_part.test",
    "test/sql/window/test_window_distinct.test",
    "test/sql/copy/csv/glob/read_csv_glob.test",
    "test/sql/copy/csv/afl/test_afl_null_padding.test",
    "test/sql/copy/csv/test_comment_midline.test",
    "test/sql/copy/csv/test_null_padding_union.test",
    "test/sql/copy/parquet/test_parquet_gzip.test",
    "test/sql/copy/partitioned/skip_partition_column_writes.test",
    "test/sql/types/struct/struct_distinct.test",
    "test/sql/types/enum/test_enum_to_numbers.test",
    "test/sql/storage/compression/dict_fsst/test_dict_fsst_with_smaller_block_size.test",
    "test/sql/storage/compression/bitpacking/bitpacking_filter_pushdown.test",
    "test/issues/monetdb/analytics10.test",
    "test/issues/general/test_16524.test",
    "test/sql/collate/test_collate_between.test",
    "test/sql/catalog/comment_on_dependencies.test",
    "test/sql/catalog/comment_on_column.test",
    "test/sql/catalog/comment_on.test",
    "test/sql/catalog/comment_on_extended.test",
    "test/sql/catalog/comment_on_pg_description.test",
    "test/sql/pragma/test_show_tables.test",
    "test/sql/alter/test_alter_if_exists.test",
    "test/sql/index/create_index_options.test",
    "test/sql/types/decimal/large_decimal_constants.test",
    "test/sql/types/uhugeint/test_uhugeint_conversion.test",
    "test/sql/types/hugeint/test_hugeint_conversion.test",
    "test/sql/catalog/test_set_search_path.test",
    "test/sql/catalog/function/test_macro_issue_13104.test",
    "test/sql/catalog/function/test_macro_relpersistence_conflict.test",
    "test/sql/catalog/function/test_recursive_macro.test",
    "test/sql/catalog/function/test_recursive_macro_no_dependency.test",
    "test/sql/catalog/test_set_schema.test",
    "test/sql/function/list/lambdas/arrow/lambdas_and_functions_deprecated.test",
    "test/sql/function/list/lambdas/lambdas_and_functions.test"
    "test/sql/storage/types/struct/nested_struct_storage.test",
    "test/sql/storage/compression/test_using_compression.test",
    "test/sql/pg_catalog/system_functions.test",
    "test/sql/copy/parquet/bloom_filters.test",
    "test/sql/copy/parquet/corrupt_stats.test",
    "test/sql/copy/file_size_bytes.test",
    "test/sql/create/create_table_compression.test",
    "test/sql/aggregate/aggregates/test_state_export.test",
    "test/sql/parallelism/intraquery/depth_first_evaluation_union_and_join.test",
    "test/sql/window/test_constant_orderby.test",
    "test/sql/tpcds/tpcds_sf0.test",
    "test/sql/copy/csv/test_glob_reorder_lineitem.test",
    "test/sql/tpch/dbgen_error.test",
    "test/sql/optimizer/plan/test_filter_pushdown_materialized_cte.test",
    "test/sql/function/timestamp/test_icu_datediff.test",
    "test/sql/function/timestamp/test_icu_datepart.test",
    "test/sql/function/timestamp/test_icu_datesub.test",
    "test/sql/copy/csv/test_csv_timestamp_tz.test",
    "test/sql/types/enum/test_5983.test",
    "test/sql/json/scalar/test_json_transform.test",
    "test/optimizer/joins/tpcds_nofail.test",
    "test/optimizer/join_dependent_filter.test",
    "test/fuzzer/duckfuzz/null_arguments.test",
    "test/sql/logging/file_system_logging.test",
    "test/sql/index/create_index_options.test",
    "test/sql/parallelism/intraquery/test_parallel_nested_aggregates.test",
    "test/sql/catalog/test_set_search_path.test",
    "test/sql/catalog/sequence/test_duckdb_sequences.test",
    "test/sql/function/numeric/set_seed_for_sample.test",
    "test/sql/function/list/lambdas/arrow/test_deprecated_lambda.test",
    "test/sql/function/list/lambdas/lambdas_and_functions.test",
    "test/sql/window/test_volatile_independence.test",
    "test/sql/copy/parquet/parquet_hive.test",
    "test/sql/table_function/duckdb_prepared_statements.test",
    "test/sql/pragma/test_query_log.test"
]

parser = argparse.ArgumentParser(description='Run tests one by one with optional flags.')
parser.add_argument('unittest_program', help='Path to the unittest program')
parser.add_argument('--no-exit', action='store_true', help='Execute all tests, without stopping on first error')
parser.add_argument('--fast-fail', action='store_true', help='Terminate on first error')
parser.add_argument('--profile', action='store_true', help='Enable profiling')
parser.add_argument('--no-assertions', action='store_false', help='Disable assertions')
parser.add_argument('--time_execution', action='store_true', help='Measure and print the execution time of each test')
parser.add_argument('--list', action='store_true', help='Print the list of tests to run')
parser.add_argument('--summarize-failures', action='store_true', help='Summarize failures', default=None)
parser.add_argument(
    '--tests-per-invocation', type=int, help='The amount of tests to run per invocation of the runner', default=1
)
parser.add_argument(
    '--print-interval', action='store', help='Prints "Still running..." every N seconds', default=300.0, type=float
)
parser.add_argument(
    '--timeout',
    action='store',
    help='Add a timeout for each test (in seconds, default: 3600s - i.e. one hour)',
    default=3600,
    type=valid_timeout,
)
parser.add_argument('--valgrind', action='store_true', help='Run the tests with valgrind', default=False)
parser.add_argument('--exclude_list', action='store_true', help='List of to-be-skipped tests')
parser.add_argument('--skip_compiled', action='store_true', help='Skip compiled tests', default=False)

args, extra_args = parser.parse_known_args()

if not args.unittest_program:
    parser.error('Path to unittest program is required')

# Access the arguments
unittest_program = args.unittest_program
no_exit = args.no_exit
fast_fail = args.fast_fail
tests_per_invocation = args.tests_per_invocation

if no_exit:
    if fast_fail:
        print("--no-exit and --fast-fail can't be combined")
        exit(1)

profile = args.profile
assertions = args.no_assertions
time_execution = args.time_execution
timeout = args.timeout
summarize_failures = args.summarize_failures
if summarize_failures is None:
    # get from env
    summarize_failures = False
    if 'SUMMARIZE_FAILURES' in os.environ:
        summarize_failures = os.environ['SUMMARIZE_FAILURES'] == '1'
    elif 'CI' in os.environ:
        # enable by default in CI if not set explicitly
        summarize_failures = True

# Use the '-l' parameter to output the list of tests to run
proc = subprocess.run([unittest_program, '-l'] + extra_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout = proc.stdout.decode('utf8').strip()
stderr = proc.stderr.decode('utf8').strip()
if len(stderr) > 0:
    print("Failed to run program " + unittest_program)
    print("Returncode:", proc.returncode)
    print(stdout)
    print(stderr)
    exit(1)

# The output is in the format of 'PATH\tGROUP', we're only interested in the PATH portion
test_cases = []
first_line = True
for line in stdout.splitlines():
    if first_line:
        first_line = False
        continue
    if len(line.strip()) == 0:
        continue
    splits = line.rsplit('\t', 1)
    test_cases.append(splits[0])

test_count = len(test_cases)
if args.list:
    for test_number, test_case in enumerate(test_cases):
        print(print(f"[{test_number}/{test_count}]: {test_case}"))

all_passed = True


def fail():
    global all_passed
    all_passed = False
    if fast_fail:
        exit(1)


def parse_assertions(stdout):
    for line in stdout.splitlines():
        if 'All tests were skipped' in line:
            return "SKIPPED"
        if line == 'assertions: - none -':
            return "0 assertions"

        # Parse assertions in format
        pos = line.find("assertion")
        if pos != -1:
            space_before_num = line.rfind(' ', 0, pos - 2)
            return line[space_before_num + 2 : pos + 10]

    return "ERROR"


is_active = False


def get_test_name_from(text):
    match = re.findall(r'\((.*?)\)\!', text)
    return match[0] if match else ''


def get_clean_error_message_from(text):
    match = re.split(r'^=+\n', text, maxsplit=1, flags=re.MULTILINE)
    return match[1] if len(match) > 1 else text


def print_interval_background(interval):
    global is_active
    current_ticker = 0.0
    while is_active:
        time.sleep(0.1)
        current_ticker += 0.1
        if current_ticker >= interval:
            print("Still running...")
            current_ticker = 0


def launch_test(test, list_of_tests=False):
    for thisline in lines:
        if thisline in test:
            return

    global is_active
    # start the background thread
    is_active = True
    background_print_thread = threading.Thread(target=print_interval_background, args=[args.print_interval])
    background_print_thread.start()

    unittest_stdout = sys.stdout if list_of_tests else subprocess.PIPE
    unittest_stderr = subprocess.PIPE

    start = time.time()
    try:
        test_cmd = [unittest_program] + test
        if args.valgrind:
            test_cmd = ['valgrind'] + test_cmd
        # should unset SUMMARIZE_FAILURES to avoid producing exceeding failure logs
        env = os.environ.copy()
        # pass env variables globally
        if list_of_tests or no_exit or tests_per_invocation:
            env['SUMMARIZE_FAILURES'] = '0'
            env['NO_DUPLICATING_HEADERS'] = '1'
        else:
            env['SUMMARIZE_FAILURES'] = '0'
        res = subprocess.run(test_cmd, stdout=unittest_stdout, stderr=unittest_stderr, timeout=timeout, env=env)
    except subprocess.TimeoutExpired as e:
        if list_of_tests:
            print("[TIMED OUT]", flush=True)
        else:
            print(" (TIMED OUT)", flush=True)
        test_name = test[0] if not list_of_tests else str(test)
        error_msg = f'TIMEOUT - exceeded specified timeout of {timeout} seconds'
        new_data = {"test": test_name, "return_code": 1, "stdout": '', "stderr": error_msg}
        error_container.append(new_data)
        fail()
        return

    stdout = res.stdout.decode('utf8') if not list_of_tests else ''
    stderr = res.stderr.decode('utf8')

    if len(stderr) > 0:
        # when list_of_tests test name gets transformed, but we can get it from stderr
        test_name = test[0] if not list_of_tests else get_test_name_from(stderr)
        error_message = get_clean_error_message_from(stderr)
        new_data = {"test": test_name, "return_code": res.returncode, "stdout": stdout, "stderr": error_message}
        error_container.append(new_data)

    end = time.time()

    # join the background print thread
    is_active = False
    background_print_thread.join()

    additional_data = ""
    if assertions:
        additional_data += " (" + parse_assertions(stdout) + ")"
    if args.time_execution:
        additional_data += f" (Time: {end - start:.4f} seconds)"
    print(additional_data, flush=True)
    if profile:
        print(f'{test_case}	{end - start}')
    if res.returncode is None or res.returncode == 0:
        return

    print("FAILURE IN RUNNING TEST")
    print(
        """--------------------
RETURNCODE
--------------------"""
    )
    print(res.returncode)
    print(
        """--------------------
STDOUT
--------------------"""
    )
    print(stdout)
    print(
        """--------------------
STDERR
--------------------"""
    )
    print(stderr)

    # if a test closes unexpectedly (e.g., SEGV), test cleanup doesn't happen,
    # causing us to run out of space on subsequent tests in GH Actions (not much disk space there)
    duckdb_unittest_tempdir = os.path.join(
        os.path.dirname(unittest_program), '..', '..', '..', 'duckdb_unittest_tempdir'
    )
    if os.path.exists(duckdb_unittest_tempdir) and os.listdir(duckdb_unittest_tempdir):
        shutil.rmtree(duckdb_unittest_tempdir)
    fail()


def run_tests_one_by_one():
    for test_number, test_case in enumerate(test_cases):
        if not profile:
            print(f"[{test_number}/{test_count}]: {test_case}", end="", flush=True)
        launch_test([test_case])


def escape_test_case(test_case):
    return test_case.replace(',', '\\,')


def run_tests_batched(batch_count):
    tmp = tempfile.NamedTemporaryFile()
    # write the test list to a temporary file
    with open(tmp.name, 'w') as f:
        for test_case in test_cases:
            f.write(escape_test_case(test_case) + '\n')
    # use start_offset/end_offset to cycle through the test list
    test_number = 0
    while test_number < len(test_cases):
        # gather test cases
        next_entry = test_number + batch_count
        if next_entry > len(test_cases):
            next_entry = len(test_cases)

        launch_test(['-f', tmp.name, '--start-offset', str(test_number), '--end-offset', str(next_entry)], True)
        test_number = next_entry


if args.tests_per_invocation == 1:
    run_tests_one_by_one()
else:
    assertions = False
    run_tests_batched(args.tests_per_invocation)

if all_passed:
    exit(0)
if summarize_failures and len(error_container):
    print(
        '''\n\n====================================================
================  FAILURES SUMMARY  ================
====================================================\n
'''
    )
    for i, error in enumerate(error_container.get_errors(), start=1):
        print(f"\n{i}:", error["test"], "\n")
        print(error["stderr"])
exit(1)
