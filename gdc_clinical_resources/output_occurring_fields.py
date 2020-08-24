import requests
import time
from common_etl.utils import has_fatal_error


def request_from_api(start_index, batch_size, expand_fields, endpoint):
    request_params = {
        'from': start_index,
        'size': batch_size,
        'expand': expand_fields
    }

    # retrieve and parse a "page" (batch) of case objects
    res = requests.post(url=endpoint, data=request_params)

    # return response body if request was successful
    if res.status_code == requests.codes.ok:
        return res
    else:
        has_fatal_error("API request returned result code {}, exiting.".
                        format(res.status_code))


def retrieve_and_output_cases(batch_size, endpoint, expand_fields):
    start_time = time.time()  # for benchmarking
    total_cases_count = 0
    is_last_page = False
    curr_index = 0
    keys = set()

    while not is_last_page:
        res = request_from_api(curr_index, batch_size, expand_fields, endpoint)
        res_json = res.json()['data']
        cases_json = res_json['hits']

        # Currently, if response doesn't contain this metadata,
        # it indicates an invalid response or request.
        if 'pagination' not in res_json:
            has_fatal_error("'pagination' not found in API response, exiting.")

        batch_record_count = res_json['pagination']['count']
        total_cases_count = res_json['pagination']['total']
        curr_page = res_json['pagination']['page']
        last_page = res_json['pagination']['pages']

        for case in cases_json:
            if 'days_to_index' in case:
                print("Found days_to_index!\n{}".format(case))
            for field in case.copy():
                keys.add(field)

        if curr_page == last_page:
            is_last_page = True

        print("API call {}".format(curr_page))
        curr_index += batch_record_count

    print("All Keys: ")
    print(keys)
    # calculate processing time and file size
    total_time = time.time() - start_time

    print(
        "\nClinical data retrieval complete!"
        "\n\t{} of {} cases retrieved"
        "\n\t{:.0f} sec to retrieve from GDC API\n".
        format(curr_index, total_cases_count, total_time)
    )


def main():
    batch_size = 2500
    expand_fields = (
        'demographic,diagnoses,diagnoses.treatments,diagnoses.annotations,'
        'exposures,family_histories,follow_ups,follow_ups.molecular_tests')
    endpoint = 'https://api.gdc.cancer.gov/cases'
    retrieve_and_output_cases(batch_size, endpoint, expand_fields)


if __name__ == '__main__':
    main()
