"""
Copyright 2023, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import sys
import time

from cda_bq_etl.utils import load_config, has_fatal_error, create_dev_table_id, format_seconds
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_study_query() -> str:
    return """
        SELECT s.embargo_date,
            s.study_name,
            s.study_submitter_id,
            s.submitter_id_name,
            s.pdc_study_id,
            s.study_id,
            s.analytical_fraction,
            STRING_AGG(sdt.disease_type, ';') AS disease_type,
            STRING_AGG(sps.primary_site, ';') AS primary_site,
            s.acquisition_type,
            s.experiment_type,
            proj.project_id,
            proj.project_submitter_id,
            pproj.project_name,
            prog.program_id,
            prog.program_submitter_id,
            prog.program_name,
            prog.program_manager,
            prog.start_date,
            prog.end_date
        FROM `isb-project-zero.cda_pdc_raw.study_`
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    # code here

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
