/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.query;

import lombok.SneakyThrows;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.transport.client.Client;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KNNQueryBuilderSourceTests extends KNNTestCase {

    private static final String FIELD_NAME = "embedding";
    private static final float[] QUERY_VECTOR = new float[] { 1.0f, 2.0f, 3.0f, 4.0f };

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        final List<NamedWriteableRegistry.Entry> entries = ClusterModule.getNamedWriteables();
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, KNNQueryBuilder.NAME, KNNQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new));
        return new NamedWriteableRegistry(entries);
    }

    public void testBuilder_whenSourceOnly_thenSucceeds() {
        QueryVectorSource source = new QueryVectorSource("products", "p1", FIELD_NAME, "graphrq_c0");
        KNNQueryBuilder builder = KNNQueryBuilder.builder().fieldName(FIELD_NAME).queryVectorSource(source).k(10).build();
        assertNull(builder.vector());
        assertEquals(source, builder.getQueryVectorSource());
    }

    public void testQueryVectorSource_whenIdMissing_thenException() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new QueryVectorSource("products", null, FIELD_NAME, null)
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains("requires id"));
    }

    @SneakyThrows
    public void testDoRewrite_whenSource_thenResolvesVectorAndRouting() {
        QueryVectorSource source = new QueryVectorSource("products", "p1", FIELD_NAME, "graphrq_c0");
        KNNQueryBuilder original = KNNQueryBuilder.builder().fieldName(FIELD_NAME).queryVectorSource(source).k(10).build();

        QueryRewriteContext context = mockSourceRewriteContext(Map.of(FIELD_NAME, List.of(1.0, 2.0, 3.0, 4.0), "graphrq_c0", "c0-7"));

        KNNQueryBuilder waiting = (KNNQueryBuilder) original.rewrite(context);
        assertNull(waiting.vector());
        assertEquals(source, waiting.getQueryVectorSource());

        KNNQueryBuilder resolved = (KNNQueryBuilder) waiting.rewrite(context);
        assertArrayEquals(QUERY_VECTOR, (float[]) resolved.vector(), 0.0f);
        assertNull(resolved.getQueryVectorSource());
        assertTrue(resolved.getFilter() instanceof BoolQueryBuilder);
        BoolQueryBuilder bool = (BoolQueryBuilder) resolved.getFilter();
        assertEquals(1, bool.filter().size());
        assertTrue(bool.filter().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder term = (TermQueryBuilder) bool.filter().get(0);
        assertEquals("graphrq_c0", term.fieldName());
        assertEquals("c0-7", term.value());
        assertEquals(1, bool.mustNot().size());
        assertTrue(bool.mustNot().get(0) instanceof IdsQueryBuilder);
        assertTrue(((IdsQueryBuilder) bool.mustNot().get(0)).ids().contains("p1"));
    }

    @SneakyThrows
    public void testDoRewrite_whenSourceDocumentMissing_thenException() {
        QueryVectorSource source = new QueryVectorSource("products", "missing", FIELD_NAME, null);
        KNNQueryBuilder original = KNNQueryBuilder.builder().fieldName(FIELD_NAME).queryVectorSource(source).k(10).build();
        QueryRewriteContext context = mock(QueryRewriteContext.class);
        Client client = mock(Client.class);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            BiConsumer<Client, ActionListener<?>> action = invocation.getArgument(0);
            action.accept(client, ActionListener.wrap(r -> {}, e -> { throw new RuntimeException(e); }));
            return null;
        }).when(context).registerAsyncAction(any());

        Exception exception = expectThrows(Exception.class, () -> original.rewrite(context));
        assertTrue(rootMessage(exception), rootMessage(exception).contains("was not found"));
    }

    @SneakyThrows
    public void testDoToQuery_whenSourceUnresolved_thenException() {
        QueryVectorSource source = new QueryVectorSource("products", "p1", FIELD_NAME, null);
        KNNQueryBuilder builder = KNNQueryBuilder.builder().fieldName(FIELD_NAME).queryVectorSource(source).k(10).build();
        Exception exception = expectThrows(IllegalStateException.class, () -> builder.doToQuery(mock(QueryShardContext.class)));
        assertTrue(exception.getMessage(), exception.getMessage().contains("must be rewritten"));
    }

    @SneakyThrows
    public void testSerialization_whenSource_thenRoundTrips() {
        QueryVectorSource source = new QueryVectorSource("products", "p1", FIELD_NAME, "graphrq_c0");
        KNNQueryBuilder original = KNNQueryBuilder.builder().fieldName(FIELD_NAME).queryVectorSource(source).k(10).build();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry())) {
                QueryBuilder deserialized = in.readNamedWriteable(QueryBuilder.class);
                assertEquals(original, deserialized);
            }
        }
    }

    @SneakyThrows
    public void testDoRewrite_whenSourceIndexOmitted_thenUsesSearchIndex() {
        QueryVectorSource source = new QueryVectorSource(null, "p1", FIELD_NAME, null);
        KNNQueryBuilder original = KNNQueryBuilder.builder().fieldName(FIELD_NAME).queryVectorSource(source).k(10).build();
        QueryCoordinatorContext context = mock(QueryCoordinatorContext.class);
        when(context.convertToCoordinatorContext()).thenReturn(context);
        when(context.getSearchRequest()).thenReturn(new SearchRequest("products"));
        Client client = mock(Client.class);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsMap()).thenReturn(Map.of(FIELD_NAME, List.of(1.0, 2.0, 3.0, 4.0)));
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            BiConsumer<Client, ActionListener<?>> action = invocation.getArgument(0);
            action.accept(client, ActionListener.wrap(r -> {}, e -> { throw new RuntimeException(e); }));
            return null;
        }).when(context).registerAsyncAction(any());

        original.rewrite(context);
        org.mockito.ArgumentCaptor<GetRequest> captor = org.mockito.ArgumentCaptor.forClass(GetRequest.class);
        verify(client).get(captor.capture(), any());
        assertEquals("products", captor.getValue().index());
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getMessage() == null ? "" : current.getMessage();
    }

    private static QueryRewriteContext mockSourceRewriteContext(Map<String, Object> sourceMap) {
        QueryRewriteContext context = mock(QueryRewriteContext.class);
        Client client = mock(Client.class);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsMap()).thenReturn(sourceMap);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            BiConsumer<Client, ActionListener<?>> action = invocation.getArgument(0);
            action.accept(client, ActionListener.wrap(r -> {}, e -> { throw new RuntimeException(e); }));
            return null;
        }).when(context).registerAsyncAction(any());
        return context;
    }
}
