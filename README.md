storm-s3
========

Storm - S3

(Inspired by [Storm HDFS](https://github.com/ptgoetz/storm-hdfs))

## Why

- allows you to interact with Amazon S3 from storm

## Building

- in order to build the project ensure that you have ~/.aws/credentials with your S3 credentials under the name [aws-testing]

## Testing

Tests run in two phases. The first phase is "local only" tests and is the only phase that is run by default, e.g.

```
 $ mvn test
```

These tests to not connect to outside resources. You can disable these tests with the ```-DskipLocalTests``` flag.

### Second Phase Tests - S3

The second phase are tests that interact with s3. They will create/delete files and buckets in an S3 account. Generally, this is a small amount of data, but it still costs you money, so they are _disabled by default_.

You can enabled then with the ```-Ds3Tests``` flag. Note, this flag will work concurrently with ```-DskipLocalTests``` flag as well.

By enabling this flag, you allow the tests to use the credentials stored at ```~/.aws/credentials``` to be used for the span of the tests. See [S3DependentTests](src/test/java/org/apache/storm/s3/S3DependentTests) for more information about the setup of the credentials file and the necessary permissions you need to give that user.

### Test Errata

There are a couple of command line flags that you can use to modify test execution:

 * ```test.exclude.pattern``` - excludes tests with the given pattern, eg. **/*Test.java excludes all tests that end in 'Test'. These tests are excluded from both phases. By default, no tests are excluded
 * ```test.output.to.file``` - determines where the test output is written. Default: ```true```, test output is written to files in target/surefire-reports.

The flags are enabled by specifying: -D<param name>[=param value>
