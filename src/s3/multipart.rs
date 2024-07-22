use aws_sdk_s3 as s3;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use s3::primitives::ByteStream;
use std::path::Path;
use std::time::Instant;

const MIN_PART_SIZE_5MB: usize = 5_242_880;
const MAX_PART_SIZE_5GB: usize = 5_368_709_120;
const DEFAULT_PART_SIZE: usize = 8_388_608;
const MAX_PARTS: usize = 10_000;

async fn partition(path: impl AsRef<Path>, mut chunk_amt: Option<usize>) -> Result<Vec<ByteStream>, ()> {
    //prepares the object for multipart upload by partitioning its data into chunks
    let mut chunks: Vec<ByteStream> = Vec::new();
    let bytestream = ByteStream::from_path(path)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let bytelen = bytestream.len();

    if chunk_amt.is_none() && bytelen / DEFAULT_PART_SIZE <= MAX_PARTS {
        chunk_amt = Some(bytelen / DEFAULT_PART_SIZE);
    }
    let chunksize = bytelen / chunk_amt.unwrap();
    if chunksize < MIN_PART_SIZE_5MB || chunksize > MAX_PART_SIZE_5GB {
        return Err(());
    }

    let mut chunk_ctr = 0;
    for _ in 0..chunk_amt.unwrap() {
        let chunk = &bytestream[chunk_ctr..chunk_ctr + chunksize].to_vec();
        let chunk_bytestream = ByteStream::from(chunk.to_owned());
        chunks.push(chunk_bytestream);
        chunk_ctr += chunksize;
    }

    let last_chunk_size = bytelen - chunk_ctr;
    if last_chunk_size > 0 {
        let chunk = &bytestream[chunk_ctr..bytelen].to_vec();
        let chunk_bytestream = ByteStream::from(chunk.to_owned());
        chunks.push(chunk_bytestream);
    }

    Ok(chunks)
}

pub async fn multipart_upload_beta(
    client: &s3::Client,
    bucket: &str,
    key: &str,
    chunk_amt: usize,
) -> Result<(), s3::Error> {
    let start_time = Instant::now();
    let multipart_init_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await;

    let upload_id = multipart_init_response?.upload_id.unwrap();
    let mut completed_parts: Vec<CompletedPart> = Vec::new();
    let (chunks, mut part_number) = (partition(key, Some(chunk_amt)).await.unwrap(), 1);

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

        let part = CompletedPart::builder()
            .set_part_number(Some(part_number))
            .set_e_tag(Some(upload_part_response.e_tag.unwrap()))
            .build();

        completed_parts.push(part);
        part_number += 1;
    }

    let completed_multipart_upload = CompletedMultipartUpload::builder()
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

pub async fn multipart_upload(
    client: &s3::Client,
    bucket: &str,
    key: &str,
    mut chunk_amt: Option<usize>,
) -> Result<(), s3::Error> {
    let mut completed_parts: Vec<CompletedPart> = Vec::new();
    let mut part_number = 1;
    
    //begin the multipart process
    let start_time = Instant::now();
    let multipart_init_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    let upload_id = multipart_init_response?.upload_id.unwrap();
    
    //collect the bytes from the object
    let bytestream = ByteStream::from_path(key)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    let bytelen = bytestream.len();

    if chunk_amt.is_none() && bytelen / DEFAULT_PART_SIZE <= MAX_PARTS {
        chunk_amt = Some(bytelen / DEFAULT_PART_SIZE);
    }

    let chunksize = bytelen / chunk_amt.unwrap();

    let mut chunk_ctr = 0;
    for _idx in 0..chunk_amt.unwrap() {
        let chunk = &bytestream[chunk_ctr..chunk_ctr + chunksize].to_vec();
        let upload_part_response = client
            .upload_part()
            .body(chunk.to_owned().into())
            .bucket(bucket)
            .key(key)
            .part_number(part_number)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let part = CompletedPart::builder()
            .set_part_number(Some(part_number))
            .set_e_tag(Some(upload_part_response.e_tag.unwrap()))
            .build();

        completed_parts.push(part);
        part_number += 1;
        chunk_ctr += chunksize;
    }

    let completed_multipart_upload = CompletedMultipartUpload::builder()
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

#[cfg(test)]
mod tests {
    use crate::s3::multipart;
    use aws_config::meta::region::RegionProviderChain;
    use aws_config::BehaviorVersion;
    use aws_sdk_s3;
    use aws_types::region::Region;
    use std::env;



    #[tokio::test]
    async fn multipart_test() -> std::io::Result<()> {
        let aws_bucket = std::env::var("AWSBUCKET").unwrap();
        let region_provider =
            RegionProviderChain::first_try(env::var("AWSREGION").ok().map(Region::new))
                .or_default_provider()
                .or_else(Region::new("us-east-1"));
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;

        let client = aws_sdk_s3::Client::new(&config);
        let _ = multipart::multipart_upload(&client, &aws_bucket, "multipart-test.txt", None).await.unwrap(); 
        Ok(())
    }
}

