class remote_client : Lwt_io.input_channel * Lwt_io.output_channel -> Arakoon_client.client
