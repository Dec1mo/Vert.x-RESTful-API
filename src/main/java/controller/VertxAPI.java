package controller;

import java.util.Map;
import java.util.Set;

import dao.UserDAO;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Cookie;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class VertxAPI extends AbstractVerticle{
	@Override
	public void start(Future<Void>fu) {	
		
		UserDAO.getUserDAO().createSomeData();
		
		Router router = Router.router(vertx);
		
		router.get("/api/users").handler(UserDAO.getUserDAO()::getAll);
		
		router.route("/api/users*").handler(BodyHandler.create());
		router.post("/api/users").handler(UserDAO.getUserDAO()::addOne);
		
		router.delete("/api/users/:id").handler(UserDAO.getUserDAO()::deleteOne);
		
		router.put("/api/users/:id").handler(UserDAO.getUserDAO()::updateOne);
		
		router.get("/api/users/:id").handler(UserDAO.getUserDAO()::getOne);
		
		router.route("/assets/*").handler(StaticHandler.create("assets"));
		
		
		
		vertx
			.createHttpServer()
			.requestHandler(router::accept)
			.listen(8083, result -> {
				if (result.succeeded()) {
					System.out.println("Success!!!");
					fu.complete();
				} else {
					System.out.println("Fail!!!");
					fu.fail(result.cause());
				}
			});
	}
	


}
