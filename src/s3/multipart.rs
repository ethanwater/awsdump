const MIN_PART_SIZE_5MB: usize = 5_242_880;

async fn chunkify(file: &str, chunk_amt: usize) -> Result<Vec<ByteStream>, ()> {
    let filestream = ByteStream::from_path(file).await.unwrap();
    let filedata = filestream.collect().await.unwrap().into_bytes();
    let mut chunks: Vec<ByteStream> = Vec::new();

    let chunk_size = filedata.len() / chunk_amt;
    if chunk_size < MIN_PART_SIZE_5MB {
        eprintln!("chunk is too small");
        return Err(());
    }

    let mut chunk_ctr = 0;
    for _ in 0..chunk_amt {
        let chunk = &filedata[chunk_ctr..chunk_ctr + chunk_size].to_vec();
        //dbg!(&chunk.len());
        let chunk_bytestream = ByteStream::from(chunk.to_owned());
        chunks.push(chunk_bytestream);
        chunk_ctr += chunk_size;
    }

    let last_chunk_size = filedata.len() - chunk_ctr;
    if last_chunk_size > 0 {
        let chunk = &filedata[chunk_ctr..filedata.len()].to_vec();
        //dbg!(&chunk.len());
        let chunk_bytestream = ByteStream::from(chunk.to_owned());
        chunks.push(chunk_bytestream);
        chunk_ctr += chunk_size;
    }
    Ok(chunks)
}

async fn multipart_upload(
    client: &s3::Client,
    bucket: &str,
    key: &str,
    chunk_amt: usize,
) -> Result<(), s3::Error> {
    let start_time = std::time::Instant::now();
    let multipart_init_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await;

    let upload_id = multipart_init_response?.upload_id.unwrap();
    let mut completed_parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();
    let (chunks, mut part_number) = (chunkify(key, chunk_amt).await.unwrap(), 1);

    for chunk in chunks {
        let upload_part_response = client
            .upload_part()
            .body(chunk.into())
            .bucket(bucket)
            .key(key)
            .part_number(part_number)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let part = aws_sdk_s3::types::CompletedPart::builder()
            .set_part_number(Some(part_number))
            .set_e_tag(Some(upload_part_response.e_tag.unwrap()))
            .build();

        completed_parts.push(part);
        part_number += 1;
    }

    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    let multipart_close_response = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await;

    match multipart_close_response {
        Ok(_) => {
            println!(
                "success: {key} -> {bucket} | time elapsed: {:?}",
                start_time.elapsed()
            );
        }
        Err(err) => {
            eprintln!("error: {:?}", err);
        }
    }
    Ok(())
}