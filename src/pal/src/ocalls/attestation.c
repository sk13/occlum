#ifndef OCCLUM_DISABLE_DCAP
#include <sgx_dcap_ql_wrapper.h>
#include <sgx_dcap_quoteverify.h>
#include <sgx_pce.h>
#include <sgx_ql_quote.h>
#include <sgx_quote_3.h>
#endif
#include "ocalls.h"

int occlum_ocall_detect_dcap_driver() {
    return access("/dev/sgx/enclave", F_OK) == 0 &&
           access("/dev/sgx/provision", F_OK) == 0 ||
           access("/dev/sgx_enclave", F_OK) == 0 &&
           access("/dev/sgx_provision", F_OK) == 0 ;
}

#define MAX_RETRY 5
quote3_error_t occlum_ocall_init_dcap_quote_generator(
    sgx_target_info_t *qe_target_info,
    uint32_t *quote_size
) {
#ifndef OCCLUM_DISABLE_DCAP
    quote3_error_t qe3_ret = SGX_QL_SUCCESS;
    int count = 0;

    while ((qe3_ret = sgx_qe_get_target_info(qe_target_info)) == SGX_QL_ERROR_BUSY &&
            count < MAX_RETRY) {
        count += 1;
        sleep(1);
    }

    if (SGX_QL_SUCCESS != qe3_ret) {
        return qe3_ret;
    }

    count = 0;
    while ((qe3_ret = sgx_qe_get_quote_size(quote_size)) == SGX_QL_ERROR_BUSY &&
            count < MAX_RETRY) {
        count += 1;
        sleep(1);
    }

    return qe3_ret;
#else
    return SGX_QL_ERROR_UNEXPECTED;
#endif
}

quote3_error_t occlum_ocall_generate_dcap_quote(
    sgx_report_t *app_report,
    uint32_t quote_size,
    uint8_t *quote_buf
) {
#ifndef OCCLUM_DISABLE_DCAP
    return sgx_qe_get_quote(app_report,
                            quote_size,
                            quote_buf);
#else
    return SGX_QL_ERROR_UNEXPECTED;
#endif
}

uint32_t occlum_ocall_get_supplement_size() {
#ifndef OCCLUM_DISABLE_DCAP
    uint32_t supplemental_data_size = 0;
    quote3_error_t dcap_ret = sgx_qv_get_quote_supplemental_data_size(
                                  &supplemental_data_size);
    if (dcap_ret == SGX_QL_SUCCESS) {
        return supplemental_data_size;
    } else {
        return 0;
    }
#else
    return 0;
#endif
}

quote3_error_t occlum_ocall_verify_dcap_quote(
    uint8_t *quote_buf,
    uint32_t quote_size,
    struct sgx_ql_qve_collateral *quote_collateral,
    time_t expiration_check_date,
    uint32_t *collateral_expiration_status,
    sgx_ql_qv_result_t *quote_verification_result,
    sgx_ql_qe_report_info_t *qve_report_info,
    uint32_t supplemental_data_size,
    uint8_t *supplemental_data
) {
#ifndef OCCLUM_DISABLE_DCAP
    return sgx_qv_verify_quote(
               quote_buf, quote_size,
               (sgx_ql_qve_collateral_t *)quote_collateral,
               expiration_check_date,
               collateral_expiration_status,
               quote_verification_result,
               qve_report_info,
               supplemental_data_size,
               supplemental_data);
#else
    return SGX_QL_ERROR_UNEXPECTED;
#endif
}
