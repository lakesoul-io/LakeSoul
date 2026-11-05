// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types.arrow;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class LakeSoulArrowTypeInfo extends TypeInformation<LakeSoulArrowWrapper> {

    private final Schema schema;

    public LakeSoulArrowTypeInfo(Schema schema) {
        this.schema = schema;
    }

    /**
     * Checks if this type information represents a basic type. Basic types are defined in {@link
     * BasicTypeInfo} and are primitives, their boxing types, Strings, Date, Void, ...
     *
     * @return True, if this type information describes a basic type, false otherwise.
     */
    @Override
    public boolean isBasicType() {
        return false;
    }

    /**
     * Checks if this type information represents a Tuple type. Tuple types are subclasses of the
     * Java API tuples.
     *
     * @return True, if this type information describes a tuple type, false otherwise.
     */
    @Override
    public boolean isTupleType() {
        return false;
    }

    /**
     * Gets the arity of this type - the number of fields without nesting.
     *
     * @return Gets the number of fields in this type without nesting.
     */
    @Override
    public int getArity() {
        return schema.getFields().size();
    }

    /**
     * Gets the number of logical fields in this type. This includes its nested and transitively
     * nested fields, in the case of composite types. In the example above, the OuterType type has
     * three fields in total.
     *
     * <p>The total number of fields must be at least 1.
     *
     * @return The number of fields in this type, including its sub-fields (for composite types)
     */
    @Override
    public int getTotalFields() {
        return schema.getFields().size();
    }

    /**
     * Gets the class of the type represented by this type information.
     *
     * @return The class of the type represented by this type information.
     */
    @Override
    public Class<LakeSoulArrowWrapper> getTypeClass() {
        return LakeSoulArrowWrapper.class;
    }

    /**
     * Checks whether this type can be used as a key. As a bare minimum, types have to be hashable
     * and comparable to be keys.
     *
     * @return True, if the type can be used as a key, false otherwise.
     */
    @Override
    public boolean isKeyType() {
        return false;
    }

    /**
     * Creates a serializer for the type. The serializer may use the ExecutionConfig for
     * parameterization.
     *
     * @param config The config used to parameterize the serializer.
     * @return A serializer for this type.
     */
    @Override
    public TypeSerializer<LakeSoulArrowWrapper> createSerializer(ExecutionConfig config) {
        return new LakeSoulArrowSerializer();
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    /**
     * Returns true if the given object can be equaled with this object. If not, it returns false.
     *
     * @param obj Object which wants to take part in the equality relation
     * @return true if obj can be equaled with this, otherwise false
     */
    @Override
    public boolean canEqual(Object obj) {
        return false;
    }
}
