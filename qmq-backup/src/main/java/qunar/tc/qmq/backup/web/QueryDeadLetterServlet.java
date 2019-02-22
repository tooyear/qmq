package qunar.tc.qmq.backup.web;

import qunar.tc.qmq.backup.model.Page;
import qunar.tc.qmq.backup.service.IndexService;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Created by zhaohui.yu
 * 2/22/19
 */
public class QueryDeadLetterServlet extends HttpServlet {

    private final IndexService indexService;

    public QueryDeadLetterServlet(IndexService indexService) {
        this.indexService = indexService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        AsyncContext asyncContext = req.startAsync();
        String subject = req.getParameter("subject");
        long start = Long.valueOf(req.getParameter("start"));
        long end = Long.valueOf(req.getParameter("end"));
        int limit = Integer.parseInt(req.getParameter("limit"));

        CompletableFuture<Page> completableFuture = indexService.scan(subject, start, end, null, 10);
        completableFuture.thenAccept((p) -> {

            asyncContext.complete();
        });
    }
}
