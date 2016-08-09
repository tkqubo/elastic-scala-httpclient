package jp.co.bizreach.elasticsearch4s;

import org.elasticsearch.action.*;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;

public class QueryBuilderClient extends AbstractClient {

    public QueryBuilderClient() {
        super(Settings.settingsBuilder().build(), null, null);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
        void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
    }

    @Override
    public void close() {
    }

}
