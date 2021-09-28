-record(state, {
    correlation_id,
    socket,
    %% packets might be split over multiple tcp packets. This buffer holds incomplete packets.
    rx_buf = <<>>,
    %% FIXME clean-up if caller dies
    pending_requests = #{},
    subscriptions = #{},
    subscriptions_by_stream = #{},
    publishers = #{},
    publishers_by_stream = #{},
    heartbeat
}).
