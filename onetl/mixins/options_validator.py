from typing import Dict
from logging import getLogger

log = getLogger(__name__)
# FORBIDDEN_JDBC_PROPERTIES - properties that are not allowed to be overridden
# in the constructors of the DWReader and DBWriter classes
FORBIDDEN_JDBC_PROPERTIES = {"user", "password", "url", "driver"}

# JDBC_PROPERTIES - these are the parameters that will go into the properties parameter
# in the spark method pyspark.sql.DataFrame.write.jdbc
JDBC_PROPERTIES = [
    "user",
    "password",
    "driver",
    "fetchsize",
    "batchsize",
    "isolationLevel",
    "sessionInitStatement",
    "truncate",
    "createTableOptions",
    "createTableColumnTypes",
    "customSchema",
]


class JDBCParamsCreatorMixin:
    def __init__(self):
        self.user = ""
        self.password = ""
        self.driver = ""
        self.spark = ""
        self.url = ""

    def jdbc_params_creator(
        self,
        jdbc_options: Dict,
    ) -> Dict:
        options = jdbc_options.copy()

        # Method intended to check the passed parameter jdbc_options.
        # The check is needed so that the parameters defined in the DBCOnnection class are not
        # overridden in the DBReader and DBWriter classes
        if set(jdbc_options).intersection(FORBIDDEN_JDBC_PROPERTIES):
            raise ValueError(
                f"An invalid parameter "
                f"<{set(jdbc_options).intersection(FORBIDDEN_JDBC_PROPERTIES)}> "
                f"was entered."
                f"{FORBIDDEN_JDBC_PROPERTIES} parameters can be defined only in DBConnection object",
            )

        options["user"] = self.user
        options["password"] = self.password
        options["driver"] = self.driver
        options["url"] = self.url

        # Have to replace the <partitionColumn> parameter with <column>
        # since the method takes the named <column> parameter
        # link to source below
        # https://git.io/JKOku
        if options.get("partitionColumn"):
            options["column"] = options["partitionColumn"]
            options.pop("partitionColumn")

        # Further below, the parameters are distributed to those that are inside the <properties> parameter
        # and those that are not included in it
        properties = {}
        for k, v in options.items():
            options[k] = str(v)
            if k in JDBC_PROPERTIES:
                properties[k] = str(v)

        for k in properties.keys():
            options.pop(k)

        options["properties"] = properties

        # Will add lower and upper bounds parameters if numPartitions parameter was set
        # If numPartitions was not set then jdbc_options will remain unchanged
        return options

    def set_lower_upper_bound(
        self,
        jdbc_options: Dict,
        table: str,
    ) -> Dict:
        """
        If numPatition was set then sets upperBound and lowerBound if not set ones.
        """

        # If the lowerBound and upperBound parameters are not set,
        # then the automatic calculation algorithm of the boundary is started.

        jdbc_options = jdbc_options.copy()

        partition_column = jdbc_options.get("column")
        num_partitions = jdbc_options.get("numPartitions")
        if num_partitions and not partition_column:
            raise ValueError("<partitionColumn> task parameter wasn't specified")

        upper_bound = jdbc_options.get("upperBound")
        lower_bound = jdbc_options.get("lowerBound")

        if (not lower_bound or not upper_bound) and num_partitions:

            if not self.spark:
                raise ValueError("Spark session not provided")

            log.info("Getting <upperBound> and <lowerBound> options")

            query_upper_lower_bound = (
                f"(SELECT min({partition_column}) lower_bound,"
                f"max({partition_column}) upper_bound "
                f"FROM {table} ) T"
            )
            log.debug(f"SQL query\n{query_upper_lower_bound}")

            jdbc_options_bounds_query = jdbc_options.copy()
            jdbc_options_bounds_query["table"] = query_upper_lower_bound
            jdbc_options_bounds_query.pop("numPartitions", None)
            jdbc_options_bounds_query.pop("lowerBound", None)
            jdbc_options_bounds_query.pop("upperBound", None)
            jdbc_options_bounds_query.pop("column", None)
            df_upper_lower_bound = self.spark.read.jdbc(**jdbc_options_bounds_query)

            # The sessionInitStatement parameter is removed because it only needs to be applied once.
            jdbc_options["properties"].pop("sessionInitStatement", None)

            tuple_upper_lower_bound = df_upper_lower_bound.collect()[0]

            if not lower_bound:
                jdbc_options["lowerBound"] = tuple_upper_lower_bound.lower_bound
                log.warning(
                    "<lowerBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.lower_bound}>",
                )

            if not upper_bound:
                jdbc_options["upperBound"] = tuple_upper_lower_bound.upper_bound
                log.warning(
                    f"<upperBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.upper_bound}>",
                )
        return jdbc_options
