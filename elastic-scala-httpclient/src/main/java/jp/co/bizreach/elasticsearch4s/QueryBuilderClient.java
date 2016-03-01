package jp.co.bizreach.elasticsearch4s;

import org.elasticsearch.action.*;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class QueryBuilderClient extends AbstractClient {

    @Override
    public Settings settings() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public AdminClient admin() {
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, Client> action, Request request) {
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {

    }

    @Override
    public ThreadPool threadPool() {
        return null;
    }
}
