package org.apache.flink.lakesoul.substrait;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SubstraitFlinkUtilTest {

    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-10-23T16:30:45.123456789Z"), ZONE);

    @Test
    public void foldsCurrentDateWithSessionTimeZone() {
        CallExpression currentDate = currentDateCall();
        ResolvedExpression folded = SubstraitFlinkUtil.foldQueryStartFunctions(currentDate, CLOCK);

        assertThat(folded).isInstanceOf(ValueLiteralExpression.class);
        assertThat(((ValueLiteralExpression) folded).getValueAs(LocalDate.class))
                .contains(LocalDate.of(2026, 10, 24));
    }

    @Test
    public void foldsCurrentTime() {
        CallExpression currentTime =
                call(
                        BuiltInFunctionDefinitions.CURRENT_TIME,
                        Collections.emptyList(),
                        DataTypes.TIME(0).notNull());
        ResolvedExpression folded = SubstraitFlinkUtil.foldQueryStartFunctions(currentTime, CLOCK);

        assertThat(((ValueLiteralExpression) folded).getValueAs(LocalTime.class))
                .contains(LocalTime.of(0, 30, 45));
    }

    @Test
    public void foldsCurrentTimestampToInstant() {
        CallExpression currentTimestamp =
                call(
                        BuiltInFunctionDefinitions.CURRENT_TIMESTAMP,
                        Collections.emptyList(),
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull());
        ResolvedExpression folded =
                SubstraitFlinkUtil.foldQueryStartFunctions(currentTimestamp, CLOCK);

        assertThat(((ValueLiteralExpression) folded).getValueAs(Instant.class))
                .contains(Instant.parse("2026-10-23T16:30:45.123Z"));
    }

    @Test
    public void foldsLocalTimestampWithoutTimeZone() {
        CallExpression localTimestamp =
                call(
                        BuiltInFunctionDefinitions.LOCAL_TIMESTAMP,
                        Collections.emptyList(),
                        DataTypes.TIMESTAMP(3).notNull());
        ResolvedExpression folded =
                SubstraitFlinkUtil.foldQueryStartFunctions(localTimestamp, CLOCK);

        assertThat(((ValueLiteralExpression) folded).getValueAs(LocalDateTime.class))
                .contains(LocalDateTime.of(2026, 10, 24, 0, 30, 45, 123_000_000));
    }

    @Test
    public void foldsNowToInstant() {
        CallExpression now =
                call(
                        BuiltInFunctionDefinitions.NOW,
                        Collections.emptyList(),
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull());
        ResolvedExpression folded = SubstraitFlinkUtil.foldQueryStartFunctions(now, CLOCK);

        assertThat(((ValueLiteralExpression) folded).getValueAs(Instant.class))
                .contains(Instant.parse("2026-10-23T16:30:45.123Z"));
    }

    @Test
    public void foldsUnixTimestampWithoutArguments() {
        CallExpression unixTimestamp =
                call(
                        BuiltInFunctionDefinitions.UNIX_TIMESTAMP,
                        Collections.emptyList(),
                        DataTypes.BIGINT());
        ResolvedExpression folded =
                SubstraitFlinkUtil.foldQueryStartFunctions(unixTimestamp, CLOCK);

        assertThat(((ValueLiteralExpression) folded).getValueAs(Long.class))
                .contains(Instant.parse("2026-10-23T16:30:45.123456789Z").getEpochSecond());
    }

    @Test
    public void doesNotFoldCurrentRowTimestamp() {
        CallExpression currentRowTimestamp =
                call(
                        BuiltInFunctionDefinitions.CURRENT_ROW_TIMESTAMP,
                        Collections.emptyList(),
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull());
        ResolvedExpression folded =
                SubstraitFlinkUtil.foldQueryStartFunctions(currentRowTimestamp, CLOCK);

        assertThat(folded).isSameAs(currentRowTimestamp);
    }

    @Test
    public void foldsCurrentDateInsidePartitionFilter() {
        FieldReferenceExpression statDate =
                new FieldReferenceExpression("stat_date", DataTypes.DATE(), 0, 0);
        CallExpression filter =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(statDate, currentDateCall()),
                        DataTypes.BOOLEAN());

        ResolvedExpression folded = SubstraitFlinkUtil.foldQueryStartFunctions(filter, CLOCK);

        assertThat(folded).isInstanceOf(CallExpression.class);
        CallExpression foldedCall = (CallExpression) folded;
        assertThat(foldedCall.getResolvedChildren().get(1))
                .isInstanceOf(ValueLiteralExpression.class);
        assertThat(
                        SubstraitFlinkUtil.filterAllPartitionColumn(
                                folded, Collections.singleton("stat_date")))
                .isTrue();

        List<ResolvedExpression> filters = Collections.singletonList(folded);
        Map<String, String> equalityFields = SubstraitFlinkUtil.equalityFilterFieldNames(filters);
        assertThat(equalityFields).containsEntry("stat_date", "2026-10-24");
    }

    private static CallExpression currentDateCall() {
        return call(
                BuiltInFunctionDefinitions.CURRENT_DATE,
                Collections.emptyList(),
                DataTypes.DATE().notNull());
    }

    private static CallExpression call(
            org.apache.flink.table.functions.BuiltInFunctionDefinition definition,
            List<ResolvedExpression> children,
            DataType outputType) {
        return CallExpression.permanent(definition, children, outputType);
    }
}
