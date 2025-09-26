import pyarrow as pa
import pyarrow.compute as pc
from lakesoul.logging import init_logger
from lakesoul.arrow import lakesoul_dataset
import decimal

# init_logger("debug")
val = pa.array([decimal.Decimal("1500.00")], type=pa.decimal128(15, 2))
filter = (pc.field("p_retailprice") >= val[0]) & (pc.field("p_size") == 50)
cols = ["p_name"]
scanner = lakesoul_dataset("part").scanner(cols, filter=filter)
table = scanner.to_table()
print(len(table))
print(table.column_names)


val = pa.array([decimal.Decimal("1500.00")], type=pa.decimal128(15, 2))
filter = (pc.field("p_retailprice") >= val[0]) & (pc.field("p_size") == 50)
fragment = list(lakesoul_dataset("part").get_fragments(filter=filter))[0]
scanner = fragment.scanner()
table = scanner.to_table()

print(len(table))
