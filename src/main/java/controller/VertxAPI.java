package controller;

import dao.UserDAO;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class VertxAPI extends AbstractVerticle{
	@Override
	public void start(Future<Void>fu) {	
		
//		UserDAO.getInstance().createSomeData();
		
		UserDAO.getInstance().initData();
		
		Router router = Router.router(vertx);
		
		router.get("/api/users").handler(UserDAO.getInstance()::getAll);
		
		router.route("/api/users*").handler(BodyHandler.create());
		router.post("/api/users").handler(UserDAO.getInstance()::addOne);
		
		router.delete("/api/users/:id").handler(UserDAO.getInstance()::deleteOne);
		
		router.put("/api/users/:id").handler(UserDAO.getInstance()::updateOne);
		
		router.get("/api/users/:id").handler(UserDAO.getInstance()::getOne);
		
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
