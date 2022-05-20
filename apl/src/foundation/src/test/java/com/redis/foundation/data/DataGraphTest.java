/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.redis.foundation.data;

import com.redis.foundation.io.DataGraphJSON;
import com.redis.foundation.std.StrUtl;
import com.redis.foundation.io.DataGraphCSV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;

public class DataGraphTest
{
	@Before
	public void setup()
	{
	}

	public void exercise1()
	{
		DataGraph dataGraph = new DataGraph("Simple Graph (Item/None)");
		try
		{
			dataGraph.addVertex("Google");
			dataGraph.addVertex("Wikipedia");
			dataGraph.addVertex("JGraphT");

			dataGraph.addEdge("JGraphT", "Wikipedia");
			dataGraph.addEdge("Google", "JGraphT");
			dataGraph.addEdge("Google", "Wikipedia");
			dataGraph.addEdge("Wikipedia", "Google");

			System.out.printf("List of Vertex Names%n");
			DataItem dataItem = dataGraph.getVertexItemByName("Google");
			Iterator<DataItem> diIterator = dataGraph.depthFirstIterator(dataItem);
			while (diIterator.hasNext())
			{
				dataItem = diIterator.next();
				System.out.printf("%s%n", dataItem.getName());
			}
		}
		catch (Exception e)
		{
			System.err.printf("Exception: %s%n", e.getMessage());
		}
	}

	public void exercise2()
	{
		DataGraph dataGraph = new DataGraph("Simple Graph (Item/None)");
		DataItem diGoogle = new DataItem.Builder().name("Google").value("http://www.google.com").build();
		DataItem diWikipedia = new DataItem.Builder().name("Wikipedia").value("http://www.wikipedia.org").build();
		DataItem diJGraphT = new DataItem.Builder().name("JGraphT").value("http://www.jgrapht.org").build();
		try
		{
			dataGraph.addVertex(diGoogle);
			dataGraph.addVertex(diWikipedia);
			dataGraph.addVertex(diJGraphT);

			dataGraph.addEdge(diJGraphT, diWikipedia);
			dataGraph.addEdge(diGoogle, diJGraphT);
			dataGraph.addEdge(diGoogle, diWikipedia);
			dataGraph.addEdge(diWikipedia, diGoogle);

			System.out.printf("List of Vertex Names & Values%n");
			dataGraph.getVertexItemStream().forEach(di -> System.out.printf("%s - %s%n", di.getName(), di.getValue()));
		}
		catch (Exception e)
		{
			System.err.printf("Exception: %s%n", e.getMessage());
		}
	}

	public void exercise3()
	{
		DataItem srcVertex, dstVertex;

		DataGraph dataGraph = new DataGraph("Site Graph", Data.GraphStructure.SimpleGraph, Data.GraphData.ItemItem);
		DataItem diGoogle = new DataItem.Builder().name("Google").value("http://www.google.com").build();
		DataItem diWikipedia = new DataItem.Builder().name("Wikipedia").value("http://www.wikipedia.org").build();
		DataItem diJGraphT = new DataItem.Builder().name("JGraphT").value("http://www.jgrapht.org").build();
		try
		{
			dataGraph.addVertex(diGoogle);
			dataGraph.addVertex(diWikipedia);
			dataGraph.addVertex(diJGraphT);

			DataItem dataItem = new DataItem.Builder().name("LinksTo").value("Internet").build();
			dataGraph.addEdge(diJGraphT, diWikipedia, dataItem);
			dataGraph.addEdge(diGoogle, diJGraphT, dataItem);
			dataGraph.addEdge(diGoogle, diWikipedia, dataItem);
			dataGraph.addEdge(diWikipedia, diGoogle, dataItem);

			System.out.printf("List of Vertexes%n");
			dataItem = dataGraph.getVertexItemByName("Google");
			Iterator<DataItem> diIterator = dataGraph.depthFirstIterator(dataItem);
			while (diIterator.hasNext())
			{
				dataItem = diIterator.next();
				System.out.printf("%s - %s%n", dataItem.getName(), dataItem.getValue());
			}

			System.out.printf("List of Edge Relationships%n");
			for (DataGraphEdge dge : dataGraph.getEdgeSet())
			{
				srcVertex = dataGraph.getEdgeItemSource(dge);
				dstVertex = dataGraph.getEdgeItemDestination(dge);
				System.out.printf("Source '%s' - %s - Destination '%s'%n", srcVertex.getName(), dge.getName(), dstVertex.getName());
			}
			System.out.printf("List of Edge Names%n");
			dataGraph.getEdgeStream().forEach(dge -> System.out.printf("%s%n", dge.getName()));

			DataGraphCSV dataGraphCSV = new DataGraphCSV(dataGraph);
			dataGraphCSV.save(dataGraph, "data");

			dataGraphCSV = new DataGraphCSV();
			Optional<DataGraph> optDataGraph = dataGraphCSV.load("data/graph_ver_sigr_ii_site_graph.csv", "data/graph_edg_sigr_ii_site_graph.csv");
			if (optDataGraph.isPresent())
			{
				System.out.printf("List of Vertexes%n");
				dataItem = dataGraph.getVertexItemByName("Google");
				diIterator = dataGraph.depthFirstIterator(dataItem);
				while (diIterator.hasNext())
				{
					dataItem = diIterator.next();
					System.out.printf("%s - %s%n", dataItem.getName(), dataItem.getValue());
				}

				System.out.printf("List of Edge Relationships%n");
				for (DataGraphEdge dge : dataGraph.getEdgeSet())
				{
					srcVertex = dataGraph.getEdgeItemSource(dge);
					dstVertex = dataGraph.getEdgeItemDestination(dge);
					System.out.printf("Source '%s' - %s - Destination '%s'%n", srcVertex.getName(), dge.getName(), dstVertex.getName());
				}
				System.out.printf("List of Edge Names%n");
				dataGraph.getEdgeStream().forEach(dge -> System.out.printf("%s%n", dge.getName()));
			}

			DataGraphJSON dataGraphJSON = new DataGraphJSON();
			dataGraphJSON.save("data/graph_movie.json", dataGraph);

			optDataGraph = dataGraphJSON.load("data/graph_movie.json");
			if (optDataGraph.isPresent())
			{
				System.out.printf("List of Vertexes%n");
				dataItem = dataGraph.getVertexItemByName("Google");
				diIterator = dataGraph.depthFirstIterator(dataItem);
				while (diIterator.hasNext())
				{
					dataItem = diIterator.next();
					System.out.printf("%s - %s%n", dataItem.getName(), dataItem.getValue());
				}

				System.out.printf("List of Edge Relationships%n");
				for (DataGraphEdge dge : dataGraph.getEdgeSet())
				{
					srcVertex = dataGraph.getEdgeItemSource(dge);
					dstVertex = dataGraph.getEdgeItemDestination(dge);
					System.out.printf("Source '%s' - %s - Destination '%s'%n", srcVertex.getName(), dge.getName(), dstVertex.getName());
				}
				System.out.printf("List of Edge Names%n");
				dataGraph.getEdgeStream().forEach(dge -> System.out.printf("%s%n", dge.getName()));
			}
		}
		catch (Exception e)
		{
			System.err.printf("Exception: %s%n", e.getMessage());
		}
	}

	// $ grep Godfather title.basics.tsv | grep movie | grep "Crime,Drama"
	private DataDoc createMovieVertex(String anId, String aTitle, String aYear, String aGenres)
	{
		DataDoc ddMovie = new DataDoc("Movie");
		ddMovie.add(new DataItem.Builder().name("id").title("Id").value(anId).build());
		ddMovie.add(new DataItem.Builder().name("title").title("Title").value(aTitle).build());
		ddMovie.add(new DataItem.Builder().name("year").title("Year").value(aYear).build());
		ArrayList<String> genreList = StrUtl.expandToList(aGenres, StrUtl.CHAR_COMMA);
		ddMovie.add(new DataItem.Builder().name("genres").title("Genres").values(genreList).build());

		return ddMovie;
	}

	// $ grep tt0068646 name.basics.tsv
	private DataDoc createPersonVertex(String anId, String aName, String aBirthYear, String aProfessions)
	{
		DataDoc ddMovie = new DataDoc("Principal");
		ddMovie.add(new DataItem.Builder().name("id").title("Id").value(anId).build());
		ddMovie.add(new DataItem.Builder().name("name").title("Name").value(aName).build());
		ddMovie.add(new DataItem.Builder().name("birth_year").title("Birth Year").value(aBirthYear).build());
		ArrayList<String> professionList = StrUtl.expandToList(aProfessions, StrUtl.CHAR_COMMA);
		ddMovie.add(new DataItem.Builder().name("professions").title("Professions").values(professionList).build());

		return ddMovie;
	}

	// $ grep tt0068646 name.basics.tsv
	private DataDoc createRoleEdge(String anId, String aRole, String aYear)
	{
		DataDoc ddMovie = new DataDoc(aRole);
		ddMovie.add(new DataItem.Builder().name("id").title("Id").value(anId).build());
		ddMovie.add(new DataItem.Builder().name("role").title("Role").value(aRole).build());
		ddMovie.add(new DataItem.Builder().name("year").title("Year").value(aYear).build());

		return ddMovie;
	}

	public void exercise4()
	{
		DataDoc srcVertex, dstVertex;

		DataGraph dataGraph = new DataGraph("Movie Graph", Data.GraphStructure.SimpleGraph, Data.GraphData.DocDoc);
		try
		{
			DataDoc ddMovieVertex = createMovieVertex("tt0068646", "The Godfather", "1972", "Crime,Drama");
			dataGraph.addVertex(ddMovieVertex);

			DataDoc ddPersonVertex = createPersonVertex("nm0000008", "Marlon Brando", "1924", "actor,soundtrack,director");
			dataGraph.addVertex(ddPersonVertex);
			DataDoc ddRoleEdge = createRoleEdge("pr000001", "Actor", "1971");
			dataGraph.addEdge(ddMovieVertex, ddPersonVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000199", "Al Pacino", "1940", "actor,producer,soundtrack");
			dataGraph.addVertex(ddPersonVertex);
			dataGraph.addEdge(ddMovieVertex, ddPersonVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000380", "Robert Duvall", "1931", "actor,producer,soundtrack");
			dataGraph.addVertex(ddPersonVertex);
			dataGraph.addEdge(ddMovieVertex, ddPersonVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000065", "Nino Rota", "1911", "composer,soundtrack,music_department");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000003", "Composer", "1971");
			dataGraph.addEdge(ddMovieVertex, ddPersonVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000338", "Francis Ford Coppola", "1939", "producer,director,writer");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000002", "Director", "1971");
			dataGraph.addEdge(ddMovieVertex, ddPersonVertex, ddRoleEdge);

			System.out.printf("List of Vertexes%n");
			for (DataDoc dataDoc : dataGraph.getVertexDocSet())
			{
				if (dataDoc.getName().equals("Movie"))
				{
					System.out.printf("%s[%s]: '%s' (%s) - %s%n", dataDoc.getName(),
									  dataDoc.getValueByName("id"), dataDoc.getValueByName("title"),
									  dataDoc.getValueByName("genres"), dataDoc.getValueByName("year"));
				}
				else
				{
					System.out.printf("%s[%s]: '%s' (%s) - %s%n", dataDoc.getName(),
									  dataDoc.getValueByName("id"), dataDoc.getValueByName("name"),
									  dataDoc.getValueByName("professions"), dataDoc.getValueByName("birth_year"));
				}
			}

			System.out.printf("List of Edge Relationships%n");
			for (DataGraphEdge dge : dataGraph.getEdgeSet())
			{
				srcVertex = dataGraph.getEdgeDocSource(dge);
				dstVertex = dataGraph.getEdgeDocDestination(dge);
				System.out.printf("Source '%s' - %s - Destination '%s'%n", srcVertex.getName(), dge.getName(), dstVertex.getName());
			}
			System.out.printf("List of Edge Names%n");
			dataGraph.getEdgeStream().forEach(dge -> System.out.printf("%s%n", dge.getName()));

			DataGraphCSV dataGraphCSV = new DataGraphCSV(dataGraph);
			dataGraphCSV.save(dataGraph, "data");

			dataGraphCSV = new DataGraphCSV();
			Optional<DataGraph> optDataGraph = dataGraphCSV.load("data/graph_ver_sigr_dd_movie_graph.csv", "data/graph_edg_sigr_dd_movie_graph.csv");
			if (optDataGraph.isPresent())
			{
				System.out.printf("List of Vertexes%n");
				for (DataDoc dataDoc : dataGraph.getVertexDocSet())
				{
					if (dataDoc.getName().equals("Movie"))
					{
						System.out.printf("%s[%s]: '%s' (%s) - %s%n", dataDoc.getName(),
										  dataDoc.getValueByName("id"), dataDoc.getValueByName("title"),
										  dataDoc.getValueByName("genres"), dataDoc.getValueByName("year"));
					}
					else
					{
						System.out.printf("%s[%s]: '%s' (%s) - %s%n", dataDoc.getName(),
										  dataDoc.getValueByName("id"), dataDoc.getValueByName("name"),
										  dataDoc.getValueByName("professions"), dataDoc.getValueByName("birth_year"));
					}
				}

				System.out.printf("List of Edge Relationships%n");
				for (DataGraphEdge dge : dataGraph.getEdgeSet())
				{
					srcVertex = dataGraph.getEdgeDocSource(dge);
					dstVertex = dataGraph.getEdgeDocDestination(dge);
					System.out.printf("Source '%s' - %s - Destination '%s'%n", srcVertex.getName(), dge.getName(), dstVertex.getName());
				}
				System.out.printf("List of Edge Names%n");
				dataGraph.getEdgeStream().forEach(dge -> System.out.printf("%s%n", dge.getName()));
			}

			DataGraphJSON dataGraphJSON = new DataGraphJSON();
			dataGraphJSON.save("data/graph_movie.json", dataGraph);
			optDataGraph = dataGraphJSON.load("data/graph_movie.json");
			if (optDataGraph.isPresent())
			{
				System.out.printf("List of Vertexes%n");
				for (DataDoc dataDoc : dataGraph.getVertexDocSet())
				{
					if (dataDoc.getName().equals("Movie"))
					{
						System.out.printf("%s[%s]: '%s' (%s) - %s%n", dataDoc.getName(),
										  dataDoc.getValueByName("id"), dataDoc.getValueByName("title"),
										  dataDoc.getValueByName("genres"), dataDoc.getValueByName("year"));
					}
					else
					{
						System.out.printf("%s[%s]: '%s' (%s) - %s%n", dataDoc.getName(),
										  dataDoc.getValueByName("id"), dataDoc.getValueByName("name"),
										  dataDoc.getValueByName("professions"), dataDoc.getValueByName("birth_year"));
					}
				}

				System.out.printf("List of Edge Relationships%n");
				for (DataGraphEdge dge : dataGraph.getEdgeSet())
				{
					srcVertex = dataGraph.getEdgeDocSource(dge);
					dstVertex = dataGraph.getEdgeDocDestination(dge);
					System.out.printf("Source '%s' - %s - Destination '%s'%n", srcVertex.getName(), dge.getName(), dstVertex.getName());
				}
				System.out.printf("List of Edge Names%n");
				dataGraph.getEdgeStream().forEach(dge -> System.out.printf("%s%n", dge.getName()));
			}
		}
		catch (Exception e)
		{
			System.err.printf("Exception: %s%n", e.getMessage());
		}
	}

	@Test
	public void exercise()
	{
		exercise1();
		exercise2();
		exercise3();
		exercise4();
	}

	@After
	public void cleanup()
	{
	}
}
