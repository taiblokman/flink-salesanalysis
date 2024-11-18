/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.taib;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.taib.dto.CategorySalesDTO;
import org.taib.entities.OrderItem;
import org.taib.entities.Product;


/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataBatchJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<OrderItem> orderItems = env
				.readCsvFile("/Users/taiblokman/decode/SalesAnalysis/Datasets/order_items.csv")
				.ignoreFirstLine()
				.pojoType(OrderItem.class,"orderItemId", "orderId", "productId", "quantity", "pricePerUnit" );
		DataSource<Product> products = env
				.readCsvFile("/Users/taiblokman/decode/SalesAnalysis/Datasets/products.csv")
				.ignoreFirstLine()
				.pojoType(Product.class,"productId", "name", "description", "price", "category" );

		// Join the datasets orderitem and products using key ProductId
		DataSet<Tuple6<String, String, Float, Integer, Float, String>> joined = orderItems
				.join(products)
				.where("productId")
				.equalTo("productId")
				.with((JoinFunction<OrderItem, Product, Tuple6<String, String, Float, Integer, Float, String>>) (first, second)
						-> new Tuple6<>(
						second.productId.toString(),
						second.name,
						first.pricePerUnit,
						first.quantity,
						first.pricePerUnit * first.quantity,
						second.category
				))
				.returns(TypeInformation.of(new TypeHint<Tuple6<String, String, Float, Integer, Float, String>>() {
				}));

		// group by category to get the total sales and count
		DataSet<CategorySalesDTO> categorySales = joined
				.map((MapFunction<Tuple6<String, String, Float, Integer, Float, String>, CategorySalesDTO>) record
						-> new CategorySalesDTO(record.f5, record.f4, 1))
				.returns(CategorySalesDTO.class)
				.groupBy("category")
				.reduce((ReduceFunction<CategorySalesDTO>) (value1, value2) ->
						new CategorySalesDTO(value1.getCategory(), value1.getTotalSales() + value2.getTotalSales(),
								value1.getCount() + value2.getCount()));

		//sort by total sales in descending order
		categorySales.sortPartition("totalSales", Order.DESCENDING).print();

		        //convert to tuple
        DataSet<Tuple3<String, Float, Integer>> conversion = categorySales
                .map((MapFunction<CategorySalesDTO, Tuple3<String, Float, Integer>>) record
                        -> new Tuple3<>(record.getCategory(), record.getTotalSales(), record.getCount()))
                .returns(new TypeHint<Tuple3<String, Float, Integer>>() {
                });

        conversion.writeAsCsv("/Users/taiblokman/decode/SalesAnalysis/output/tuple-output.csv",
                "\n", ",", FileSystem.WriteMode.OVERWRITE);

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Execute program, beginning computation.
		env.execute("Sales Analysis");
	}
}
