// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

fn main() {
    build_json_codec_service();
}

fn build_json_codec_service() {
    let token_service = tonic_build::manual::Service::builder()
        .name("TokenServer")
        .package("json.token")
        .method(
            tonic_build::manual::Method::builder()
                .name("create_token")
                .route_name("CreateToken")
                .input_type("crate::token_codec::Claims")
                .output_type("crate::token_codec::TokenResponse")
                .codec_path("crate::token_codec::JsonCodec")
                .build(),
        )
        .build();

    tonic_build::manual::Builder::new().compile(&[token_service]);
}
