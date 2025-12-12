package org.apache.flink.lakesoul.entry;


import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.bson.BsonDocument;

import java.io.Serializable;
import java.text.ParseException;
import java.util.List;


public class MyMongoSerializationSchema
        implements MongoSerializationSchema<Tuple2<Boolean, Row>>, Serializable {

    private final List<String> fieldNames;
    private final List<LogicalType> fieldTypes;
    private final List<String> structNameFieldList;

    public MyMongoSerializationSchema(Table coll) {

        RowType rowType = (RowType) coll.getResolvedSchema()
                .toPhysicalRowDataType()
                .getLogicalType();

        this.fieldNames = rowType.getFieldNames();
        this.fieldTypes = rowType.getChildren();

        // 提前拿到 structNameFieldList
        MongoSinkUtils mongoSinkUtils = new MongoSinkUtils();
        this.structNameFieldList = mongoSinkUtils.traverseRow(rowType);
    }

    @Override
    public WriteModel<BsonDocument> serialize(Tuple2<Boolean, Row> record, MongoSinkContext context) {

        Row row = record.f1;
        BsonDocument document = new BsonDocument();
        MongoSinkUtils mongoSinkUtils = new MongoSinkUtils();

        for (int i = 0; i < fieldNames.size(); i++) {
            Object fieldValue = row.getField(i);
            if (fieldValue == null) {
                continue;
            }

            try {
                document.append(
                        fieldNames.get(i),
                        mongoSinkUtils.convertTonBsonValue(fieldValue, structNameFieldList)
                );
            } catch (ParseException e) {
                throw new RuntimeException("Failed to convert field: " + fieldNames.get(i), e);
            }
        }

        return new InsertOneModel<>(document);
    }

    public List<String> getStructNameFieldList() {
        return structNameFieldList;
    }
}

