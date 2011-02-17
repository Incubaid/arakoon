val make_remote_client : string -> 
  Lwt_io.input_channel * Lwt_io.output_channel -> Arakoon_client.client Lwt.t
