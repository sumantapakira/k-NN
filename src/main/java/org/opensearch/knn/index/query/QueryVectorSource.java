/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.query;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Document that supplies the k-NN query vector (and optionally a routing term)
 * instead of the client sending the float array.
 *
 * <pre>
 * "source": {
 *   "index": "products",
 *   "id": "product-123",
 *   "field": "embedding",
 *   "routing_field": "graphrq_c0"
 * }
 * </pre>
 */
public final class QueryVectorSource implements Writeable, ToXContentObject {
    public static final ParseField NAME = new ParseField("source", "query_document");
    public static final ParseField INDEX_FIELD = new ParseField("index");
    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField ROUTING_FIELD = new ParseField("routing_field");

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME.getPreferredName(), Builder::new);
    static {
        PARSER.declareString(Builder::index, INDEX_FIELD);
        PARSER.declareString(Builder::id, ID_FIELD);
        PARSER.declareString(Builder::field, FIELD_FIELD);
        PARSER.declareString(Builder::routingField, ROUTING_FIELD);
    }

    private final String index;
    private final String id;
    private final String field;
    private final String routingField;

    public QueryVectorSource(String index, String id, String field, String routingField) {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] requires id", KNNQueryBuilder.NAME));
        }
        this.index = index;
        this.id = id;
        this.field = field;
        this.routingField = routingField;
    }

    public QueryVectorSource(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readString(), in.readOptionalString(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
        out.writeString(id);
        out.writeOptionalString(field);
        out.writeOptionalString(routingField);
    }

    public static QueryVectorSource fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (index != null) {
            builder.field(INDEX_FIELD.getPreferredName(), index);
        }
        builder.field(ID_FIELD.getPreferredName(), id);
        if (field != null) {
            builder.field(FIELD_FIELD.getPreferredName(), field);
        }
        if (routingField != null) {
            builder.field(ROUTING_FIELD.getPreferredName(), routingField);
        }
        return builder.endObject();
    }

    public String getIndex() {
        return index;
    }

    public String getId() {
        return id;
    }

    public String getField() {
        return field;
    }

    public String getRoutingField() {
        return routingField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof QueryVectorSource == false) {
            return false;
        }
        QueryVectorSource that = (QueryVectorSource) o;
        return Objects.equals(index, that.index)
            && Objects.equals(id, that.id)
            && Objects.equals(field, that.field)
            && Objects.equals(routingField, that.routingField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, field, routingField);
    }

    public static class Builder {
        private String index;
        private String id;
        private String field;
        private String routingField;

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        public Builder routingField(String routingField) {
            this.routingField = routingField;
            return this;
        }

        public QueryVectorSource build() {
            return new QueryVectorSource(index, id, field, routingField);
        }
    }
}
