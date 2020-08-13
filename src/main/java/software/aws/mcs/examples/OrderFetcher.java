package software.aws.mcs.examples;

/*-
 * #%L
 * AWS SigV4 Auth Java Driver 4.x Examples
 * %%
 * Copyright (C) 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * #L%
 */

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import software.aws.mcs.auth.SigV4AuthProvider;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class OrderFetcher {
    public static void main(String[] args) {

        SigV4AuthProvider provider = new SigV4AuthProvider(args[0]);

        List<InetSocketAddress> contactPoints = Collections.singletonList(new InetSocketAddress(args[1], 9142));

        System.out.println("contactPoints: " + contactPoints);
        System.out.println("provider: " + provider);
        System.out.println("arqs 0: " + args[0]);

        try (CqlSession session = CqlSession.builder()
                                            .addContactPoints(contactPoints)
                                            .withAuthProvider(provider)
                                            .withLocalDatacenter(args[0])
                                            .build()) {

            System.out.println("Acabou!!!");

        }catch (Exception e){
            System.out.println("DEU ERRO");
            System.out.println(e);
        }

    }

    public static void update (CqlSession session){
        Update update = QueryBuilder.update("keyspace_bff", "table_bff")
                .setColumn("description", QueryBuilder.bindMarker())
                .where(Relation.column("id").isEqualTo(QueryBuilder.bindMarker()),
                        Relation.column("name").isEqualTo(QueryBuilder.bindMarker()));

        SimpleStatement simpleStatement = update.build();
        //simpleStatement.setKeyspace("keyspace_bff");
        //simpleStatement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        PreparedStatement preparedStatement = session.prepare(simpleStatement);

        BoundStatement boundStatement = preparedStatement.bind()
                .setString("description", "update")
                .setString("id", "2")
                .setString("name", "teste do jar");

        session.execute(boundStatement);
    }

    public static void delete (CqlSession session){
        Delete delete = QueryBuilder.deleteFrom("keyspace_bff", "table_bff")
                .whereColumn("id").isEqualTo(QueryBuilder.bindMarker());

        SimpleStatement simpleStatement = delete.build();

        PreparedStatement preparedStatement = session.prepare(simpleStatement);

        BoundStatement boundStatement = preparedStatement.bind().setString("id", "3");

        session.execute(boundStatement);
    }

    public static void select (CqlSession session){

        ResultSet rs = session.execute("select * from keyspace_bff.table_bff");

        for (Row row : rs) {
            System.out.printf(row.getString("id") + " " + row.getString("name") + " " + row.getString("description") + "\n");
        }
    }

    public static void insert (CqlSession session){
        RegularInsert insert = QueryBuilder.insertInto("keyspace_bff", "table_bff")
                .value("id", QueryBuilder.bindMarker())
                .value("name", QueryBuilder.bindMarker())
                .value("description", QueryBuilder.bindMarker());

        SimpleStatement simpleStatement = insert.build();
        simpleStatement.setKeyspace("keyspace_bff");
        simpleStatement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        PreparedStatement preparedStatement = session.prepare(simpleStatement);

        BoundStatement boundStatement = preparedStatement.bind()
                .setString("id", "3")
                .setString("name", "teste do jar")
                .setString("description", "teste do jar");

        session.execute(boundStatement);

        //session.close();


    }
}