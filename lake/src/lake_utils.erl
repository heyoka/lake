-module(lake_utils).

-export([response_code_to_atom/1]).

-include("response_codes.hrl").

response_code_to_atom(?RESPONSE_OK) ->
    ok;
response_code_to_atom(?RESPONSE_STREAM_DOES_NOT_EXIST) ->
    stream_does_not_exist;
response_code_to_atom(?RESPONSE_SUBSCRIPTION_ID_ALREADY_EXISTS) ->
    subscription_id_already_exists;
response_code_to_atom(?RESPONSE_SUBSCRIPTION_ID_DOES_NOT_EXIST) ->
    subscription_id_does_not_exist;
response_code_to_atom(?RESPONSE_STREAM_ALREADY_EXISTS) ->
    stream_already_exists;
response_code_to_atom(?RESPONSE_STREAM_NOT_AVAILABLE) ->
    stream_not_available;
response_code_to_atom(?RESPONSE_SASL_MECHANISM_NOT_SUPPORTED) ->
    sasl_mechanism_not_supported;
response_code_to_atom(?RESPONSE_AUTHENTICATION_FAILURE) ->
    authentication_failure;
response_code_to_atom(?RESPONSE_SASL_ERROR) ->
    sasl_error;
response_code_to_atom(?RESPONSE_SASL_CHALLENGE) ->
    sasl_challenge;
response_code_to_atom(?RESPONSE_SASL_AUTHENTICATION_FAILURE_LOOPBACK) ->
    sasl_authentication_failure_loopback;
response_code_to_atom(?RESPONSE_VIRTUAL_HOST_ACCESS_FAILURE) ->
    virtual_host_access_failure;
response_code_to_atom(?RESPONSE_UNKNOWN_FRAME) ->
    unknown_frame;
response_code_to_atom(?RESPONSE_FRAME_TOO_LARGE) ->
    frame_too_large;
response_code_to_atom(?RESPONSE_INTERNAL_ERROR) ->
    internal_error;
response_code_to_atom(?RESPONSE_ACCESS_REFUSED) ->
    access_refused;
response_code_to_atom(?RESPONSE_PRECONDITION_FAILED) ->
    precondition_failed;
response_code_to_atom(?RESPONSE_PUBLISHER_DOES_NOT_EXIST) ->
    publisher_does_not_exist.
