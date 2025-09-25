import pyarrow as pa
import pyarrow.compute as pc
from lakesoul.logging import init_logger
from lakesoul.arrow import lakesoul_dataset
import decimal

# init_logger("debug")
# filter = (pc.field("p_size") == 50) & (pc.field("p_retailprice") >= 1500.00)
val = pa.array([decimal.Decimal("5.00")], type=pa.decimal128(15, 2))
filter = pc.field("p_retailprice") >= val[0]
scanner = lakesoul_dataset("part").scanner(filter=filter)
table = scanner.to_table()
print(len(table))
