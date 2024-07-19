mod s3;

use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3;
use aws_types::region::Region;
use std::env;

#[::tokio::main]
async fn main() -> Result<(), aws_sdk_s3::Error> {
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
    let _ = s3::multipart::multipart_upload(&client, &aws_bucket, "multipart-test.txt", 4).await?;
    Ok(())
}
