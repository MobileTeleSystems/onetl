<a id="troubleshooting"></a>

# Troubleshooting

In case of error please follow instructions below:

* Read the logs or exception messages you’ve faced with.
  : * If Python logs are note verbose enough, [increase the log level](../logging.md#logging).
    * If Spark logs are note verbose enough, [increase the log level](spark.md#troubleshooting-spark).
* Read documentation related to a class or method you are using.
* [Google](https://google.com) the error message, and carefully read the search result:
  : * [StackOverflow](https://stackoverflow.com/) answers.
    * [Spark](https://spark.apache.org/docs/latest/) documentation.
    * Documentation of database or filesystem you are connecting to.
    * Documentation of underlying connector.
* Search for known [issues](https://github.com/MobileTeleSystems/onetl/issues), or create a new one.
* Always use the most resent versions of onETL, PySpark and connector packages, [compatible with your environment](../install/spark.md#install-spark).
