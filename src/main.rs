mod s3;


#[::tokio::main]
async fn main() -> Result<(), s3::Error> {
    let aws_bucket = std::env::var("AWSBUCKET").unwrap();
    let aws_region = std::env::var("AWSREGION").unwrap();
    let region_provider =
        RegionProviderChain::first_try(env::var("AWSREGION").ok().map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    let client = s3::Client::new(&config);
    let _ = multipart_upload(&client, &aws_bucket, "multipart-test.txt", 4).await?;
    Ok(())
}
