import tempfile
import json
import dateutil.parser

from jsonschema.exceptions import ValidationError

from pyspark.sql.types import StructType, StructField, ArrayType, \
    IntegerType, StringType, BooleanType, TimestampType, BinaryType, \
    DateType, DecimalType, DoubleType, FloatType, ByteType, LongType, \
    ShortType

_SIMPLE_TYPES = [
    IntegerType, StringType, TimestampType, BinaryType, BooleanType,
    DateType, DecimalType, DoubleType, FloatType,
    ByteType, LongType, ShortType
]

def _is_simple_type(pyschema):
    for t in _SIMPLE_TYPES:
        if isinstance(pyschema, t):
            return True
    return False

STRING_TYPE  = {"type": ["string", "null"]}
LONG_TYPE    = {"type": ["integer", "null"]}
BOOLEAN_TYPE = {"type": ["boolean", "null"]}
DOUBLE_TYPE  = {"type": ["number", "null"]}

def is_datetime_string(validator, value, instance, schema):
    try:
        ts = dateutil.parser.parse(instance)
    except:
        raise ValidationError("{} is not an ISO 8601 format".format(instance))

TIMESTAMP_TYPE = {
    "type": ["string", "null"],
    "is_datetime_string": {}
}

def nullable(json_schema):
    type = json_schema['type']
    if isinstance(type, str):
        ret = json_schema.copy()
        ret['type'] = [json_schema['type'], 'null']
        return ret

    if isinstance(type, list):
        if 'null' in type:
            return json_schema
        if len(type) != 1:
            raise Exception("Unsupported schema")
        ret = json_schema.copy()
        ret['type'] = [json_schema['type'][0], 'null']
        return ret

    raise Exception("Unsupported schema")


def array_of(type, nullable=True):
    if nullable:
        return {
            "type": ["array", "null"],
            "items": type
        }
    else:
        return {
            "type": "array",
            "items": type
        }

# load jsonl file, each row comply to schema
# save to parquet file parquet_filename
class JSONConverter(object):
    def __init__(self, tmp_dir=None):
        self.tmp_dir = tmp_dir

    def json_file_to_parquet(self, json_filename, schema, parquet_filename, partitions=1):
        df = spark.read.schema(schema).json(json_filename)
        df.coalesce(partitions).write.mode("overwrite").parquet(parquet_filename)

    # save an array of json rows to parquet file
    def json_rows_to_parquet(self, spark, rows, schema, parquet_filename, partitions=1):
        with tempfile.NamedTemporaryFile(mode="w+t", dir=self.tmp_dir) as f:
            for row in rows:
                f.write("{}\n".format(json.dumps(row)))
            f.flush()

            df = spark.read.schema(schema).json(f.name)
            df.coalesce(partitions).write.mode("overwrite").parquet(parquet_filename)

def _src_type(json_schema):
    type = json_schema['type']
    if isinstance(type, str):
        return type, False

    if isinstance(type, list):
        if len(type) == 1:
            if type[0] == "null":
                raise Exception("Unsupported schema")
            return type[0], False

        if len(type) == 2:
            if type[0] == 'null':
                src_type = type[1]
            elif type[1] == 'null':
                src_type = type[0]
            else:
                src_type = None
            if src_type is None or src_type == 'null':
                raise Exception("Unsupported schema")
            return src_type, True

    # any unhanlded case is not supported
    raise Exception("Unsupported schema")

# return a tuple, 1st element is the pyspark type, 2nd is boolean specify nullable or not
def py_type_from_json_schema(json_schema):
    src_type, nullable = _src_type(json_schema)

    if src_type == 'string':
        if "is_datetime_string" in json_schema:
            return TimestampType(), nullable
        else:
            return StringType(), nullable
    if src_type == 'integer':
        return LongType(), nullable
    if src_type == 'number':
        return DoubleType(), nullable
    if src_type == 'boolean':
        return BooleanType(), nullable

    if src_type == "object":
        fields = []
        for name, sub_schema in json_schema['properties'].items():
            sub_type, sub_nullable = py_type_from_json_schema(sub_schema)
            fields.append(StructField(name, sub_type, nullable=sub_nullable))

        return StructType(fields), nullable

    if src_type == "array":
        element_type, element_nullable = py_type_from_json_schema(json_schema['items'])
        return ArrayType(element_type, element_nullable), nullable


    raise Exception("Unsupported schema")

def print_pyschema(pyschema, indent=0):
    if _is_simple_type(pyschema):
        return

    prefix = ' '*indent

    if isinstance(pyschema, ArrayType):
        print("{}element: {} (containsNull = {})".format(prefix, pyschema.elementType.typeName(), pyschema.containsNull))
        print_pyschema(pyschema.elementType, indent)
        return
    if isinstance(pyschema, StructType):
        for field in pyschema.fields:
            field_type = field.dataType
            print("{}{}: {} (nullable = {})".format(prefix, field.name, field_type.typeName(), field.nullable))
            print_pyschema(field_type, indent+4)
        return
