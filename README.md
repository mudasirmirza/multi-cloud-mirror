# Multi-Cloud-Mirror

## Description

multi-cloud-mirror provides an easy, multi-processing way of synchronizing a bucket at Amazon S3 to a container at Rackspace Cloud Files, or vice versa. It is written and maintained by Joe Masters Emison, reachable by email at joe at buildfax dot com.

## Installation

multi-cloud-mirror should work with all versions of python from 2.7 onward (tested with 2.7.1), and will probably work with earlier versions as well (potentially with some modifications). multi-cloud-mirror was developed with the following modules:

```bash
easy_install python-cloudfiles==1.7.9.1 easy_install boto==1.9b easy_install argparse
```

You will also need to install configuration files on the local system for multi-cloud-mirror to work:

**/etc/boto.cfg** (or **~/.boto**):
```ini
[Credentials]
aws_access_key_id = <aws_access_key_id>
aws_secret_access_key = <aws_secret_access_key>
```

**/etc/cloudfiles.cfg**:
```bash
[Credentials]
username = <username>
api_key = <api_key>
```

## Running Multi-Cloud-Mirror

**Options**

positional arguments: "s3://bucket->cf://container" a synchronization scenario, of the form "s3://bucket->cf://container" or "cf://container->s3://bucket"

optional arguments: -h, --help show this help message and exit --process NUMPROCESSES number of simultaneous file upload threads to run --maxsize MAXFILESIZE maximium file size to sync, in bytes (files larger than this size will be skipped) --from EMAILDEST email address from which to send the status email; must be specified to receive message --to EMAILSRC email address(es) (comma-separated) to which to send the status email; must be specificed to recieve message --subject EMAILSUBJ subject of the status email --tmpfile TMPFILE temporary file used for writing when sending from cf to s3 --debug DEBUG turn on debug output ```

### Example

```bash
./multi-cloud-mirror.py --process 3 --from 'joe@someplace.com' --to 'joe@someplace.com,joe@someotherplace.com' 's3://myS3bucket->cf://myCFContainer' 'cf://myCFContainer->s3://myOtherS3Bucket'
```

## Using Lockrun

I prefer running the Multi-Cloud-Mirror script with lockrun, which will make sure that no other instance of multi_cloud_mirror.py is running. The source code to lockrun (which is in the public domain) is available from the download page here.

Example in crontab 30 */4 * * * root /usr/local/bin/lockrun --lockfile=/var/run/mcm.lockrun -- /usr/local/bin/multi-cloud-mirror.py --process 3 --from 'joe@someplace.com' --to 'joe@someplace.com,joe@someotherplace.com' 's3://myS3bucket->cf://myCFContainer' 'cf://myCFContainer->s3://myOtherS3Bucket'

## Other Notes

* When you run the script, you may occasionally encounter this error at the end of the script: ```<type 'exceptions.TypeError'>: 'NoneType' object is not callable```. This is a known multiprocessing bug (http://bugs.python.org/issue4106.), which has not been fixed in some versions of Python/multiprocessing library. It has no impact on the successful running of the script; it doesn't come until the script is done with what it needs to do.
* This script is best used with RightScale and its companion Multi-Cloud Mirror ServerTemplate and RightScript. Additionally, I have made available [a very detailed howto guide](http://multi-cloud-mirror.googlecode.com/files/How_to_Set_up_a_Multi-Cloud_Mirror_Server.pdf).



## Project Information

Original repo: https://code.google.com/archive/p/multi-cloud-mirror/

The project was created on Oct 6, 2011.
License: Mozilla Public License 1.1
5 stars
svn-based source control
Labels: 
amazonwebservices rackspace cloudfiles s3 boto python-cloudfiles
