package com.googlecode.lazyrecords.azuretable;

import com.google.common.base.Throwables;
import com.googlecode.lazyrecords.*;
import com.googlecode.lazyrecords.azuretable.mappings.AzureTableMappings;
import com.googlecode.lazyrecords.sql.expressions.Expressible;
import com.googlecode.lazyrecords.sql.expressions.Expression;
import com.googlecode.lazyrecords.sql.expressions.Expressions;
import com.googlecode.totallylazy.*;
import com.googlecode.totallylazy.predicates.*;
import com.googlecode.lazyrecords.mappings.StringMappings;
import com.googlecode.lazyrecords.sql.expressions.SelectBuilder;
import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;
import com.microsoft.windowsazure.services.table.client.TableQuery.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.googlecode.totallylazy.Pair.pair;
import static java.lang.String.format;

public class AzureTableSequence<T> extends Sequence<T> {
    private final CloudTableClient client;
    private final TableQuery<DynamicTableEntity> tableQuery;
    private final StringMappings mappings;
    private final Callable1<? super DynamicTableEntity, T> entityToRecord;
    private final Logger logger;
    private final Value<Iterable<T>> data;

    public AzureTableSequence(CloudTableClient client, TableQuery<DynamicTableEntity> query, StringMappings mappings, Callable1<? super DynamicTableEntity, T> entityToRecord, Logger logger) {
        this.client = client;
        this.tableQuery = query;
        this.mappings = mappings;
        this.entityToRecord = entityToRecord;
        this.logger = logger;
        this.data = new Function<Iterable<T>>() {
            @Override
            public Iterable<T> call() throws Exception {
                return Computation.memorise(iterator(tableQuery));
            }
        }.lazy();
    }

    public Iterator<T> iterator() {
        return data.value().iterator();
    }

    private Iterator<T> iterator(final TableQuery<DynamicTableEntity> tableQuery) {
        return iterator(tableQuery, null).map(entityToRecord).iterator();
    }

    private Sequence<DynamicTableEntity> iterator(final TableQuery<DynamicTableEntity> tableQuery, ResultContinuation continuation) {
        try {
            ResultSegment<DynamicTableEntity> result = client.executeSegmented(tableQuery, continuation);
            Sequence<DynamicTableEntity> entities = Sequences.sequence(result.getResults());
            if (result.getHasMoreResults()) {
                return entities.join(iterator(tableQuery, result.getContinuationToken()));
            }
            return entities;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Sequence<T> filter(Predicate<? super T> predicate) {
        String filter = toFilter(predicate);
        System.out.println("*** filter " + filter);
        return new AzureTableSequence<T>(client, Strings.isEmpty(filter) ? tableQuery: tableQuery.where(filter), mappings, entityToRecord, logger);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S> Sequence<S> map(final Callable1<? super T, ? extends S> callable) {
        Callable1 raw = callable;
        if (raw instanceof Keyword) {
            final Keyword<S> keyword = (Keyword<S>) raw;
            return new AzureTableSequence<S>(client, tableQuery.select(new String[] {keyword.name()}), mappings, entityToValue(keyword), logger);
        }
        if (raw instanceof SelectCallable) {
            return (Sequence<S>) new AzureTableSequence(client, tableQuery.select(((SelectCallable) raw).keywords().map(keywordToName()).toList().toArray(new String[0])), mappings, entityToRecord, logger);
        }
        logger.log(Maps.map(pair(Loggers.TYPE, Loggers.SIMPLE_DB), pair(Loggers.MESSAGE, "Unsupported function passed to 'map', moving computation to client"), pair(Loggers.FUNCTION, callable)));
        return super.map(callable);
    }

    private <S> Function1<DynamicTableEntity, S> entityToValue(final Keyword<S> keyword) {
        return new Function1<DynamicTableEntity, S>() {
            public S call(DynamicTableEntity item) throws Exception {
                return ((Record) entityToRecord.call(item)).get(keyword);
            }
        };
    }

    private Function1<Keyword<?>, String> keywordToName() {
        return new Function1<Keyword<?>, String>() {
            public String call(Keyword<?> keyword) throws Exception {
                return keyword.name();
            }
        };
    }

    private static String toFilter(Predicate<?> predicate) {
        if(predicate instanceof AllPredicate){
            return "";
        }
        if (predicate instanceof WherePredicate) {
            WherePredicate<Record,?> wherePredicate = Unchecked.cast(predicate);
            Callable1<? super Record, ?> callable = wherePredicate.callable();
            Predicate<?> subpredicate = wherePredicate.predicate();
            if (subpredicate instanceof BetweenPredicate) {
                BetweenPredicate<?> betweenPredicate = (BetweenPredicate) subpredicate;
                String column = derivedColumn(callable);
                return TableQuery.combineFilters(column + " ge " + getValue(betweenPredicate.lower()),
                        "and", column + " le " + getValue(betweenPredicate.upper()));
            } else if (subpredicate instanceof InPredicate) {
                InPredicate<Object> inPredicate = Unchecked.cast(subpredicate);
                final String column = derivedColumn(callable);
                Sequence<Object> sequence = Sequences.sequence(inPredicate.values());
                return sequence.map(objectToEqClause(column)).toString("(", ") or (", ")");
            } else if (subpredicate instanceof StartsWithPredicate) {
                StartsWithPredicate startsWithPredicate = Unchecked.cast(subpredicate);
                final String column = derivedColumn(callable);
                final String value = startsWithPredicate.value();
                return TableQuery.combineFilters(column + " ge " + getValue(value),
                        "and", column + " lt " + getValue(String.valueOf(incrementString(value.toCharArray()))));
            }
            return derivedColumn(callable) + " " + toFilter(wherePredicate.predicate());
        }
        if (predicate instanceof AndPredicate) {
            AndPredicate<?> andPredicate = (AndPredicate) predicate;
            if(andPredicate.predicates().isEmpty()) return "";
            return TableQuery.combineFilters(toFilter(andPredicate.predicates().first()), "and", toFilter(andPredicate.predicates().second()));
        }
        if (predicate instanceof OrPredicate) {
            OrPredicate<?> orPredicate = (OrPredicate) predicate;
            if(orPredicate.predicates().isEmpty()) return "";
            return TableQuery.combineFilters(toFilter(orPredicate.predicates().first()), "or", toFilter(orPredicate.predicates().second()));
        }
        if (predicate instanceof NullPredicate) {
            return "is null";
        }
        if (predicate instanceof NotNullPredicate) {
            return "is not null";
        }
        if (predicate instanceof EqualsPredicate) {
            return "eq " + getValue(predicate);
        }
        if (predicate instanceof NotEqualsPredicate) {
            return "ne " + getValue(predicate);
        }
        if (predicate instanceof Not) {
            return "not (" + toFilter(((Not) predicate).predicate()) + ")";
        }
        if (predicate instanceof GreaterThan) {
            return "gt " + getValue(predicate);
        }
        if (predicate instanceof GreaterThanOrEqualTo) {
            return "ge " + getValue(predicate);
        }
        if (predicate instanceof LessThan) {
            return "lt " + getValue(predicate);
        }
        if (predicate instanceof LessThanOrEqualTo) {
            return "le " + getValue(predicate);
        }
        if (predicate instanceof Between) {
            throw new UnsupportedOperationException();
            /*
            Between between = (Between) predicate;
            return expression("between ? and ?", sequence(between.lower(), between.upper()));
            */
        }
        if (predicate instanceof InPredicate) {
            throw new UnsupportedOperationException();
            /*
            InPredicate<Object> inPredicate = Unchecked.cast(predicate);
            Sequence<Object> sequence = inPredicate.values();
            if (sequence instanceof Expressible) {
                Expression pair = ((Expressible) sequence).express();
                return expression("in ( " + pair.text() + ")", pair.parameters());
            }
            return expression(repeat("?").take((Integer) inPredicate.values().size()).toString("in (", ",", ")"), sequence);
            */
        }
        if (predicate instanceof StartsWithPredicate) {
            throw new UnsupportedOperationException();
            /*
            return expression("like ?", sequence((Object) (((StartsWithPredicate) predicate).value() + "%")));
            */
        }
        if (predicate instanceof EndsWithPredicate) {
            throw new UnsupportedOperationException();
            /*
            return expression("like ?", sequence((Object) ("%" + ((EndsWithPredicate) predicate).value())));
            */
        }
        if (predicate instanceof ContainsPredicate) {
            throw new UnsupportedOperationException();
            /*
            return expression("like ?", sequence((Object) ("%" + ((ContainsPredicate) predicate).value() + "%")));
            */
        }
        throw new UnsupportedOperationException("Unsupported predicate " + predicate);
    }


    public static char[] incrementString(char[] str) {
        for (int pos = str.length - 1; pos >= 0; pos--) {
            if(Character.toUpperCase(str[pos]) != 'Z') {
                str[pos]++;
                break;
            } else {
                str[pos] = 'a';
            }
        }
        return str;
    }

    private static Function1<Object, String> objectToEqClause(final String column) {
        return new Function1<Object, String>() {
            @Override
            public String call(Object o) throws Exception {
                return column + " eq " +getValue(o);
            }
        };
    }

    public static <T> String derivedColumn(Callable1<? super Record, T> callable) {
        if(callable instanceof Aggregate){
            /*
            Aggregate aggregate = (Aggregate) callable;
            return setFunctionType(aggregate.callable(), aggregate.source()).join(asClause(aggregate));
            */
            throw new UnsupportedOperationException();
        }
        if (callable instanceof AliasedKeyword) {
            /*
            AliasedKeyword aliasedKeyword = (AliasedKeyword) callable;
            return name(aliasedKeyword.source()).join(asClause(aliasedKeyword));
            */
            throw new UnsupportedOperationException();
        }
        if (callable instanceof Keyword) {
            Keyword<?> keyword = (Keyword) callable;
            return keyword.name();
        }
        if (callable instanceof SelectCallable) {
            /*
            Sequence<Keyword<?>> keywords = ((SelectCallable) callable).keywords();
            return selectList(keywords);
            */
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException("Unsupported callable " + callable);
    }

    private static String getValue(Predicate<?> predicate) {
        Object value = ((Value) predicate).value();
        return getValue(value);
    }

    private static String getValue(Object value) {
        if (value instanceof Boolean) {
            return value.toString();
        } else if (value instanceof Date) {
            DateFormat df = AzureTableMappings.DATE_FORMAT;
            String date = df.format((Date)value);
            return "datetime'" + date + "'";
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof UUID) {
            return "guid'" + value.toString() + "'";
        } else {
            return "'" + value.toString() + "'";
        }
    }

    // server-side sorting not supported by Azure tables
    /*
    @Override
    public Sequence<T> sortBy(Comparator<? super T> comparator) {
        try {
            return new AzureTableSequence<T>(client, tableQuery.orderBy(comparator), mappings, entityToRecord, logger);
        } catch (UnsupportedOperationException ex) {
            logger.log(Maps.map(pair(Loggers.TYPE, Loggers.SIMPLE_DB), pair(Loggers.MESSAGE, "Unsupported comparator passed to 'sortBy', moving computation to client"), pair(Loggers.COMPARATOR, comparator)));
            return super.sortBy(comparator);
        }
    }
    */
}
