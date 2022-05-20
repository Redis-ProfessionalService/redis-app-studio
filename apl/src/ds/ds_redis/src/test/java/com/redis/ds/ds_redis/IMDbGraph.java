package com.redis.ds.ds_redis;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGraph;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;

import java.util.ArrayList;

@SuppressWarnings("FieldCanBeLocal")
public class IMDbGraph
{
	private final String GRAPH_DB_NAME = "IMDb Graph";

	private final AppCtx mAppCtx;

	public IMDbGraph(AppCtx anAppCtx)
	{
		mAppCtx = anAppCtx;
	}

	public String fieldName(String aFieldName)
	{
		return String.format("common_%s", aFieldName);
	}

	public String fieldName(String aLabel, String aFieldName)
	{
		return String.format("%s_%s", aLabel.toLowerCase(), aFieldName);
	}

	// $ grep Godfather title.basics.tsv | grep movie | grep "Crime,Drama"
	public DataDoc createMovieVertex(String anId, String aName, String aDescription,
									 String aYear, String aGenres, double aRevenue)
	{
		String vertexLabel = "Movie";
		DataDoc ddMovie = new DataDoc(vertexLabel);
		ddMovie.add(new DataItem.Builder().name(fieldName("id")).title("Id").isRequired(true).value(anId).isPrimary(true).build());
		DataItem dataItem = new DataItem.Builder().name(fieldName("name")).title("Name").isRequired(true).value(aName).build();
		dataItem.enableFeature(Data.FEATURE_IS_GRAPH_LABEL);
		dataItem.enableFeature(Data.FEATURE_IS_GRAPH_TITLE);
		dataItem.enableFeature(Data.FEATURE_IS_SEARCH);
		dataItem.enableFeature(Data.FEATURE_IS_SUGGEST);
		ddMovie.add(dataItem);
		dataItem = new DataItem.Builder().name(fieldName("summary")).title("Summary").values(aDescription).build();
		dataItem.enableFeature(Data.FEATURE_IS_SEARCH);
		ddMovie.add(dataItem);
		ddMovie.add(new DataItem.Builder().type(Data.Type.Integer).name(fieldName(vertexLabel, "release_year")).title("Release Year").value(aYear).build());
		ArrayList<String> genreList = StrUtl.expandToList(aGenres, StrUtl.CHAR_COMMA);
		dataItem = new DataItem.Builder().name(fieldName(vertexLabel, "genres")).title("Genres").values(genreList).build();
		dataItem.enableFeature(Data.FEATURE_IS_SEARCH);
		ddMovie.add(dataItem);
		ddMovie.add(new DataItem.Builder().type(Data.Type.Double).name(fieldName(vertexLabel, "revenue")).title("Revenue").value(aRevenue).build());

		return ddMovie;
	}

	// $ grep tt0068646 name.basics.tsv
	public DataDoc createPersonVertex(String anId, String aName, String aBioSummary, String aBirthYear,
									  String aProfessions, boolean aIsMale, int aHeightInInches, String anEyeColor)
	{
		String vertexLabel = "Principal";
		DataDoc ddMovie = new DataDoc(vertexLabel);
		ddMovie.add(new DataItem.Builder().name(fieldName("id")).title("Id").isRequired(true).value(anId).isPrimary(true).build());
		DataItem dataItem = new DataItem.Builder().name(fieldName("name")).title("Name").isRequired(true).value(aName).build();
		dataItem.enableFeature(Data.FEATURE_IS_GRAPH_LABEL);
		dataItem.enableFeature(Data.FEATURE_IS_GRAPH_TITLE);
		dataItem.enableFeature(Data.FEATURE_IS_SEARCH);
		dataItem.enableFeature(Data.FEATURE_IS_SUGGEST);
		ddMovie.add(dataItem);
		dataItem = new DataItem.Builder().name(fieldName("summary")).title("Summary").values(aBioSummary).build();
		dataItem.enableFeature(Data.FEATURE_IS_SEARCH);
		ddMovie.add(dataItem);
		ddMovie.add(new DataItem.Builder().type(Data.Type.Integer).name(fieldName(vertexLabel, "birth_year")).title("Birth Year").value(aBirthYear).build());
		ArrayList<String> professionList = StrUtl.expandToList(aProfessions, StrUtl.CHAR_COMMA);
		dataItem = new DataItem.Builder().name(fieldName(vertexLabel, "professions")).title("Professions").values(professionList).build();
		dataItem.enableFeature(Data.FEATURE_IS_SEARCH);
		ddMovie.add(dataItem);
		ddMovie.add(new DataItem.Builder().type(Data.Type.Boolean).name(fieldName(vertexLabel, "is_male")).title("Is Male").value(aIsMale).build());
		ddMovie.add(new DataItem.Builder().type(Data.Type.Integer).name(fieldName(vertexLabel, "height")).title("Height").value(aHeightInInches).build());
		dataItem = new DataItem.Builder().name(fieldName(vertexLabel, "eye_color")).title("Eye Color").value(anEyeColor).build();
		dataItem.enableFeature(Data.FEATURE_IS_SEARCH);
		ddMovie.add(dataItem);

		return ddMovie;
	}

	// $ grep tt0068646 name.basics.tsv
	public DataDoc createRoleEdge(String anId, String aRole, String aYear, String aPosition, String aCharacterName)
	{
		DataDoc ddPrincipalToMovieEdge = new DataDoc(aRole);
		ddPrincipalToMovieEdge.add(new DataItem.Builder().name(fieldName("id")).title("Id").isRequired(true).value(anId).isPrimary(true).build());
		DataItem dataItem = new DataItem.Builder().name(fieldName("type")).title("Type").isRequired(true).value(aRole).build();
		dataItem.enableFeature(Data.FEATURE_IS_GRAPH_TYPE);
		ddPrincipalToMovieEdge.add(dataItem);
		dataItem = new DataItem.Builder().name("contribution").title("Contribution").value(aPosition).build();
		dataItem.enableFeature(Data.FEATURE_IS_GRAPH_TITLE);
		ddPrincipalToMovieEdge.add(dataItem);
		ddPrincipalToMovieEdge.add(new DataItem.Builder().type(Data.Type.Integer).name("employment_year").title("Employment Year").value(aYear).build());
		ddPrincipalToMovieEdge.add(new DataItem.Builder().name("character_name").title("Character Name").value(aCharacterName).build());

		return ddPrincipalToMovieEdge;
	}

	public DataGraph create(boolean anIsMultiEdge)
	{
		DataDoc ddPersonVertexSaved;

		DataGraph dataGraph = new DataGraph(GRAPH_DB_NAME, Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);
		try
		{
			DataDoc ddMovieVertex = createMovieVertex("tt0068646", "The Godfather", "An organized crime dynasty's aging patriarch transfers control of his clandestine empire to his reluctant son.",
													  "1972", "Crime,Drama", 4309000000.0);
			dataGraph.addVertex(ddMovieVertex);

			DataDoc ddPersonVertex = createPersonVertex("nm0000008", "Marlon Brando", "Marlon Brando is widely considered the greatest movie actor of all time, rivaled only by the more theatrically oriented Laurence Olivier in terms of esteem. Unlike Olivier, who preferred the stage to the screen, Brando concentrated his talents on movies after bidding the Broadway stage adieu in 1949, a decision for which he was severely criticized when his star began to dim in the 1960s and he was excoriated for squandering his talents. No actor ever exerted such a profound influence on succeeding generations of actors as did Brando. More than 50 years after he first scorched the screen as Stanley Kowalski in the movie version of Tennessee Williams' A Streetcar Named Desire (1951) and a quarter-century after his last great performance as Col. Kurtz in Francis Ford Coppola's Apocalypse Now (1979), all American actors are still being measured by the yardstick that was Brando. It was if the shadow of John Barrymore, the great American actor closest to Brando in terms of talent and stardom, dominated the acting field up until the 1970s. He did not, nor did any other actor so dominate the public's consciousness of what WAS an actor before or since Brando's 1951 on-screen portrayal of Stanley made him a cultural icon. Brando eclipsed the reputation of other great actors circa 1950, such as Paul Muni and Fredric March. Only the luster of Spencer Tracy's reputation hasn't dimmed when seen in the starlight thrown off by Brando. However, neither Tracy nor Olivier created an entire school of acting just by the force of his personality. Brando did.",
														"1924", "actor,soundtrack,director", true, 69, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			DataDoc ddRoleEdge = createRoleEdge("pr000001", "Actor", "1971", "Lead", "Don Vito Corleone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000199", "Al Pacino", "Alfredo James \"Al\" 'Pacino established himself as a film actor during one of cinema's most vibrant decades, the 1970s, and has become an enduring and iconic figure in the world of American movies. He was born April 25, 1940 in Manhattan, New York City, to Italian-American parents, Rose (nee Gerardi) and Sal Pacino.",
												"1940", "actor,producer,soundtrack", true, 68, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000002", "Actor", "1971", "Supporting", "Michael Corleone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0001735", "Talia Shire", "Talia Shire was born on April 25, 1946 in Lake Success, Long Island, New York, USA as Talia Rose Coppola. She is an actress and producer, known for Rocky (1976), The Godfather (1972) and The Godfather: Part II (1974). She was previously married to Jack Schwartzman and David Shire.",
												"1946", "actress,producer,director", false, 64, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000003", "Actor", "1971", "Supporting", "Connie Corleone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000065", "Nino Rota", "Born in Milan in 1911 into a family of musicians, Nino Rota was first a student of Orefice and Pizzetti. Then, still a child, he moved to Rome where he completed his studies at the Conservatory of Santa Cecilia in 1929 with Alfredo Casella. In the meantime, he had become an 'enfant prodige', famous both as a composer and as an orchestra conductor. His first oratorio, \"L'infanzia di San Giovanni Battista,\" was performed in Milan and Paris as early as 1923 and his lyrical comedy, \"Il Principe Porcaro,\" was composed in 1926. From 1930 to 1932, Nino Rota lived in the USA. He won a scholarship to the Curtis Institute of Philadelphia where he attended classes in composition taught by Rosario Scalero and classes in orchestra taught by Fritz Reiner. He returned to Italy and earned a degree in literature from the University of Milan. In 1937, he began a teaching career that led to the directorship of the Bari Conservatory, a title he held from 1950 until his death in 1979.",
												"1911", "composer,soundtrack,music_department", true, 72, "Hazel");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000004", "Composer", "1971", "Lead", "Composer");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000338", "Francis Ford Coppola", "Francis Ford Coppola was born in 1939 in Detroit, Michigan, but grew up in a New York suburb in a creative, supportive Italian-American family. His father, Carmine Coppola, was a composer and musician. His mother, Italia Coppola (née Pennino), had been an actress. Francis Ford Coppola graduated with a degree in drama from Hofstra University, and did graduate work at UCLA in filmmaking. He was training as assistant with filmmaker Roger Corman, working in such capacities as sound-man, dialogue director, associate producer and, eventually, director of Dementia 13 (1963), Coppola's first feature film. During the next four years, Coppola was involved in a variety of script collaborations, including writing an adaptation of \"This Property is Condemned\" by Tennessee Williams (with Fred Coe and Edith Sommer), and screenplays for Is Paris Burning? (1966) and Patton (1970), the film for which Coppola won a Best Original Screenplay Academy Award. In 1966, Coppola's 2nd film brought him critical acclaim and a Master of Fine Arts degree. In 1969, Coppola and George Lucas established American Zoetrope, an independent film production company based in San Francisco.",
												"1939", "producer,director,writer", true, 72, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000005", "Director", "1971", "Lead", "Director");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000380", "Robert Duvall", "Veteran actor and director Robert Selden Duvall was born on January 5, 1931, in San Diego, CA, to Mildred Virginia (Hart), an amateur actress, and William Howard Duvall, a career military officer who later became an admiral. Duvall majored in drama at Principia College (Elsah, IL), then served a two-year hitch in the army after graduating in 1953. He began attending The Neighborhood Playhouse School of the Theatre In New York City on the G.I. Bill in 1955, studying under Sanford Meisner along with Dustin Hoffman, with whom Duvall shared an apartment. Both were close to another struggling young actor named Gene Hackman. Meisner cast Duvall in the play \"The Midnight Caller\" by Horton Foote, a link that would prove critical to his career, as it was Foote who recommended Duvall to play the mentally disabled \"Boo Radley\" in To Kill a Mockingbird (1962). This was his first \"major\" role since his 1956 motion picture debut as an MP in Somebody Up There Likes Me (1956), starring Paul Newman.",
												"1931", "actor,producer,soundtrack", true, 69, "Black");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000006", "Actor", "1971", "Supporting", "Tom Hagen");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0078788", "Apocalypse Now", "A U.S. Army officer serving in Vietnam is tasked with assassinating a renegade Special Forces Colonel who sees himself as a god.",
											  "1979", "Drama,Mystery,War", 150000000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000007", "Actor", "1978", "Supporting", "Lieutenant Colonel Bill Kilgore");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0118632", "The Apostle", "After his happy life spins out of control, a preacher from Texas changes his name, goes to Louisiana and starts preaching on the radio.",
											  "1997", "Drama", 21000000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000008", "Actor", "1996", "Lead", "The Apostle E.F.");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0316356", "Open Range", "A former gunslinger is forced to take up arms again when he and his cattle crew are threatened by a corrupt lawman.",
											  "2003", "Action,Drama,Romance", 68300000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000009", "Actor", "2002", "Co-Lead", "Boss Spearman");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000126", "Kevin Costner", "Kevin Michael Costner was born on January 18, 1955 in Lynwood, California, the third child of Bill Costner, a ditch digger and ultimately an electric line servicer for Southern California Edison, and Sharon Costner (née Tedrick), a welfare worker. His older brother, Dan, was born in 1950. A middle brother died at birth in 1953. His father's job required him to move regularly, which caused Kevin to feel like an Army kid, always the new kid at school, which led to him being a daydreamer. As a teen, he sang in the Baptist church choir, wrote poetry, and took writing classes. At 18, he built his own canoe and paddled his way down the rivers that Lewis & Clark followed to the Pacific. Despite his present height, he was only 5'2\" when he graduated high school. Nonetheless, he still managed to be a basketball, football and baseball star. In 1973, he enrolled at California State University at Fullerton, where he majored in business. During that period, Kevin decided to take acting lessons five nights a week. He graduated with a business degree in 1978 and married his college sweetheart, Cindy Costner. He initially took a marketing job in Orange County. Everything changed when he accidentally met Richard Burton on a flight from Mexico. Burton advised him to go completely after acting if that is what he wanted. He quit his job and moved to Hollywood soon after. He drove a truck, worked on a deep sea fishing boat, and gave bus tours to stars' homes before finally making his own way into the films.",
												"1955", "actor,producer,soundtrack", true, 73, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000010", "Actor", "2002", "Co-Lead", "Charley Waite");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			if (anIsMultiEdge)
			{
				ddRoleEdge = createRoleEdge("pr000011", "Director", "2002", "Lead", "Director");
				dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			}

			ddMovieVertex = createMovieVertex("tt0099348", "Dances with Wolves", "Lieutenant John Dunbar, assigned to a remote western Civil War outpost, befriends wolves and Indians, making him an intolerable aberration in the military.",
											  "1990", "Adventure,Drama,Western", 424200000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000012", "Actor", "1989", "Lead", "Lieutenant John Dunbar");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0102798", "Robin Hood: Prince of Thieves", "When Robin and his Moorish companion come to England and the tyranny of the Sheriff of Nottingham, he decides to fight back as an outlaw.",
											  "1991", "Action,Adventure,Drama", 390500000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000013", "Actor", "1990", "Lead", "Robin of Locksley");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0103855", "The Bodyguard", "A former Secret Service agent takes on the job of bodyguard to an R&B singer, whose lifestyle is most unlike a President's.",
											  "1992", "Action,Drama,Music", 411000000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000014", "Actor", "1991",  "Co-Lead", "Frank Farmer");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			if (anIsMultiEdge)
			{
				ddRoleEdge = createRoleEdge("pr000015", "Director", "1991", "Lead", "Director");
				dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			}

			ddPersonVertex = createPersonVertex("nm0001365", "Whitney Houston", "Whitney Elizabeth Houston was born into a musical family on 9 August 1963, in Newark, New Jersey, the daughter of gospel star Cissy Houston, cousin of singing star Dionne Warwick and goddaughter of soul legend Aretha Franklin. She began singing in the choir at her church, The New Hope Baptist Church in Newark, as a young child and by the age of 15 was singing backing vocals professionally with her mother on Chaka Khan's 1978 hit, 'I'm Every Woman'. She went on to provide backing vocals for Lou Rawls, Jermaine Jackson and her own mother and worked briefly as a model, appearing on the cover of 'Seventeen' magazine in 1981.",
												"1963", "soundtrack,actress,producer", false, 68, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000016", "Actor", "1991", "Co-Lead", "Rachel Marron");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			if (anIsMultiEdge)
			{
				ddRoleEdge = createRoleEdge("pr000017", "Singer", "1991", "Lead", "Rachel Marron");
				dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			}

			ddPersonVertex = createPersonVertex("nm0000126", "Kevin Costner", "Kevin Michael Costner was born on January 18, 1955 in Lynwood, California, the third child of Bill Costner, a ditch digger and ultimately an electric line servicer for Southern California Edison, and Sharon Costner (née Tedrick), a welfare worker. His older brother, Dan, was born in 1950. A middle brother died at birth in 1953. His father's job required him to move regularly, which caused Kevin to feel like an Army kid, always the new kid at school, which led to him being a daydreamer. As a teen, he sang in the Baptist church choir, wrote poetry, and took writing classes. At 18, he built his own canoe and paddled his way down the rivers that Lewis & Clark followed to the Pacific. Despite his present height, he was only 5'2\" when he graduated high school. Nonetheless, he still managed to be a basketball, football and baseball star. In 1973, he enrolled at California State University at Fullerton, where he majored in business. During that period, Kevin decided to take acting lessons five nights a week. He graduated with a business degree in 1978 and married his college sweetheart, Cindy Costner. He initially took a marketing job in Orange County. Everything changed when he accidentally met Richard Burton on a flight from Mexico. Burton advised him to go completely after acting if that is what he wanted. He quit his job and moved to Hollywood soon after. He drove a truck, worked on a deep sea fishing boat, and gave bus tours to stars' homes before finally making his own way into the films.",
												"1955", "actor,producer,soundtrack", true, 73, "Blue");
			ddMovieVertex = createMovieVertex("tt0052522", "The Untouchables", "During the era of Prohibition in the United States, Federal Agent Eliot Ness sets out to stop ruthless Chicago gangster Al Capone and, because of rampant corruption, assembles a small, hand-picked team to help him.",
											  "1987", "Action,Drama,Romance", 76270454.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000018", "Actor", "1986", "Supporting", "Eliot Ness");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000125", "Sean Connery", "The tall, handsome and muscular Scottish actor Sean Connery is best known as the original actor to portray James Bond in the hugely successful movie franchise, starring in seven films between 1962 and 1983. Some believed that such a career-defining role might leave him unable to escape it, but he proved the doubters wrong, becoming one of the most notable film actors of his generation, with a host of great movies to his name. This arguably culminated in his greatest acclaim in 1988, when Connery won the Academy Award for Best Supporting Actor for his role as an Irish cop in The Untouchables (1987), stealing the thunder from the movie's principal star Kevin Costner. Connery was polled as \"The Greatest Living Scot\" and \"Scotland's Greatest Living National Treasure\". In 1989, he was proclaimed \"Sexiest Man Alive\" by People magazine, and in 1999, at age 69, he was voted \"Sexiest Man of the Century.\"",
												"1930", "actor,producer,soundtrack", true, 74, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000019", "Actor", "1986", "Lead", "Jim Malone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000361", "Brian De Palma", "Brian De Palma is the son of a surgeon. He studied physics but at the same time felt his dedication for the movies and made some short films. After seven independent productions he had his first success with Sisters (1972) and his voyeuristic style. Restlessly he worked on big projects with the script writers Paul Schrader, John Farris and Oliver Stone. He also filmed a novel of Stephen King: Carrie (1976). Another important film was The Untouchables (1987) with a script by David Mamet adapted from the TV series.",
												"1940", "director,writer,producer", true, 71, "Gray");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000020", "Director", "1986", "Lead", "Director");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000412", "Andy Garcia", "One of Hollywood's most private and guarded leading men, Andy Garcia has created iconic characters while at the same time staying true to his acting roots and personal projects. Garcia was born Andrés Arturo García Menéndez on April 12, 1956, in Havana, Cuba, to Amelie Menéndez, a teacher of English, and René García Núñez, an attorney and avocado farmer. Garcia's family was relatively affluent. However, when he was two years old, Fidel Castro came to power, and the family fled to Miami Beach. Forced to work menial jobs for a while, the family started a fragrance company that was eventually worth more than a million dollars. He attended Natilus Junior High School and later at Miami Beach Senior High School. Andy was a popular student in school, a good basketball player and good-looking. He dreamed of playing professional baseball. In his senior year, though, he contracted mononucleosis and hepatitis, and unable to play sports, he turned his attention to acting.",
												"1956", "actor,producer,soundtrack", true, 70, "Hazel");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000021", "Actor", "1986", "Supporting", "George Stone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000134", "Robert De Niro", "One of the greatest actors of all time, Robert De Niro was born on August 17, 1943 in Manhattan, New York City, to artists Virginia (Admiral) and Robert De Niro Sr. His paternal grandfather was of Italian descent, and his other ancestry is Irish, English, Dutch, German, and French. He was trained at the Stella Adler Conservatory and the American Workshop. De Niro first gained fame for his role in Bang the Drum Slowly (1973), but he gained his reputation as a volatile actor in Mean Streets (1973), which was his first film with director Martin Scorsese. He received an Academy Award for Best Supporting Actor for his role in The Godfather: Part II (1974) and received Academy Award nominations for best actor in Taxi Driver (1976), The Deer Hunter (1978) and Cape Fear (1991). He received the Academy Award for Best Actor for his role as Jake LaMotta in Raging Bull (1980).",
												"1943", "actor,producer,soundtrack", true, 70, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000022", "Actor", "1986", "Co-Lead", "Al Capone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0068646", "The Godfather", "An organized crime dynasty's aging patriarch transfers control of his clandestine empire to his reluctant son.",
											  "1972", "Crime,Drama", 4309000000.0);
			ddRoleEdge = createRoleEdge("pr000023", "Actor", "1971", "Supporting", "Vito Corleone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0060009", "Mission: Impossible", "An American agent, under false suspicion of disloyalty, must discover and expose the real spy without the help of his organization.",
											  "1996", "Action,Drama,Thriller;Adventure", 45769799.0);
			dataGraph.addVertex(ddMovieVertex);
			ddPersonVertex = createPersonVertex("nm0000129", "Tom Cruise", "In 1976, if you had told fourteen-year-old Franciscan seminary student Thomas Cruise Mapother IV that one day in the not too distant future he would be Tom Cruise, one of the top 100 movie stars of all time, he would have probably grinned and told you that his ambition was to join the priesthood. Nonetheless, this sensitive, deeply religious youngster who was born in 1962 in Syracuse, New York, was destined to become one of the highest paid and most sought after actors in screen history. Tom is the only son (among four children) of nomadic parents, Mary Lee (Pfeiffer), a special education teacher, and Thomas Cruise Mapother III, an electrical engineer. His parents were both from Louisville, Kentucky, and he has German, Irish, and English ancestry. Young Tom spent his boyhood always on the move, and by the time he was 14 he had attended 15 different schools in the U.S. and Canada. He finally settled in Glen Ridge, New Jersey with his mother and her new husband. While in high school, Tom wanted to become a priest but pretty soon he developed an interest in acting and abandoned his plans of becoming a priest, dropped out of school, and at age 18 headed for New York and a possible acting career. The next 15 years of his life are the stuff of legends. He made his film debut with a small part in Endless Love (1981) and from the outset exhibited an undeniable box office appeal to both male and female audiences.",
												"1962", "actor,producer,soundtrack", true, 67, "Green");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000024", "Actor", "1995", "Lead", "Ethan Hunt");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			if (anIsMultiEdge)
			{
				ddRoleEdge = createRoleEdge("pr000025", "Producer", "1995", "Lead", "Producer");
				dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			}

			ddPersonVertex = createPersonVertex("nm0000361", "Brian De Palma", "Brian De Palma is the son of a surgeon. He studied physics but at the same time felt his dedication for the movies and made some short films. After seven independent productions he had his first success with Sisters (1972) and his voyeuristic style. Restlessly he worked on big projects with the script writers Paul Schrader, John Farris and Oliver Stone. He also filmed a novel of Stephen King: Carrie (1976). Another important film was The Untouchables (1987) with a script by David Mamet adapted from the TV series.",
												"1940", "director,writer,producer", true, 71, "Gray");
			ddRoleEdge = createRoleEdge("pr000026", "Director", "1995", "Lead", "Director");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000685", "Jon Voight", "Jon Voight is an American actor of German and Slovak descent. He has won the Academy Award for Best Actor in a Leading Role for his role as paraplegic Vietnam War veteran Luke Martin in the war film \"Coming Home\" (1978). He has also been nominated for the same award other two times. He was first nominated for his role as aspiring gigolo Joe Buck in \"Midnight Cowboy\" (1969), He was last nominated for the award for his role as escaped convict Oscar \"Manny\" Manheim in \"Runaway Train\" (1985). He was also nominated for the Academy Award for Best Actor in a Supporting Role, for his role as sports journalist Howard Cosell (1918-1995) in \"Ali\" (2001).",
												"1938", "actor,producer,writer", true, 74, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000027", "Actor", "1995", "Supporting", "Jim Phelps");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000384", "Danny Elfman", "As Danny Elfman was growing up in the Los Angeles area, he was largely unaware of his talent for composing. It wasn't until the early 1970s that Danny and his older brother Richard Elfman started a musical troupe while in Paris; the group \"Mystic Knights of Oingo-Boingo\" was created for Richard's directorial debut, Forbidden Zone (1980) (now considered a cult classic by Elfman fans). The group's name went through many incarnations over the years, beginning with \"The Mystic Knights of the Oingo Boingo\" and eventually just Oingo Boingo. While continuing to compose eclectic, intelligent rock music for his L.A.-based band (some of which had been used in various film soundtracks, e.g. Weird Science (1985)), Danny formed a friendship with young director Tim Burton, who was then a fan of Oingo Boingo. Danny went on to score the soundtrack of Pee-wee's Big Adventure (1985), Danny's first orchestral film score. The Elfman-Burton partnership continued (most notably through the hugely-successful \"Batman\" flicks) and opened doors of opportunity for Danny, who has been referred to as \"Hollywood's hottest film composer\".",
												"1953", "music_department,soundtrack,composer", true, 65, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000028", "Composer", "1995", "Lead", "Composer");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0790724", "Jack Reacher", "A homicide investigator digs deeper into a case involving a trained military sniper who shot five random victims.",
											  "2012", "Action,Drama,Thriller", 218000000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddPersonVertex = createPersonVertex("nm0000129", "Tom Cruise", "In 1976, if you had told fourteen-year-old Franciscan seminary student Thomas Cruise Mapother IV that one day in the not too distant future he would be Tom Cruise, one of the top 100 movie stars of all time, he would have probably grinned and told you that his ambition was to join the priesthood. Nonetheless, this sensitive, deeply religious youngster who was born in 1962 in Syracuse, New York, was destined to become one of the highest paid and most sought after actors in screen history. Tom is the only son (among four children) of nomadic parents, Mary Lee (Pfeiffer), a special education teacher, and Thomas Cruise Mapother III, an electrical engineer. His parents were both from Louisville, Kentucky, and he has German, Irish, and English ancestry. Young Tom spent his boyhood always on the move, and by the time he was 14 he had attended 15 different schools in the U.S. and Canada. He finally settled in Glen Ridge, New Jersey with his mother and her new husband. While in high school, Tom wanted to become a priest but pretty soon he developed an interest in acting and abandoned his plans of becoming a priest, dropped out of school, and at age 18 headed for New York and a possible acting career. The next 15 years of his life are the stuff of legends. He made his film debut with a small part in Endless Love (1981) and from the outset exhibited an undeniable box office appeal to both male and female audiences.",
												"1962", "actor,producer,soundtrack", true, 67, "Green");
			ddRoleEdge = createRoleEdge("pr000029", "Actor", "2011", "Lead", "Jack Reacher");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000380", "Robert Duvall", "Veteran actor and director Robert Selden Duvall was born on January 5, 1931, in San Diego, CA, to Mildred Virginia (Hart), an amateur actress, and William Howard Duvall, a career military officer who later became an admiral. Duvall majored in drama at Principia College (Elsah, IL), then served a two-year hitch in the army after graduating in 1953. He began attending The Neighborhood Playhouse School of the Theatre In New York City on the G.I. Bill in 1955, studying under Sanford Meisner along with Dustin Hoffman, with whom Duvall shared an apartment. Both were close to another struggling young actor named Gene Hackman. Meisner cast Duvall in the play \"The Midnight Caller\" by Horton Foote, a link that would prove critical to his career, as it was Foote who recommended Duvall to play the mentally disabled \"Boo Radley\" in To Kill a Mockingbird (1962). This was his first \"major\" role since his 1956 motion picture debut as an MP in Somebody Up There Likes Me (1956), starring Paul Newman.",
												"1931", "actor,producer,soundtrack", true, 69, "Black");
			ddRoleEdge = createRoleEdge("pr000030", "Actor", "2011", "Supporting", "Cash");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt1631867", "Edge of Tomorrow", "A soldier fighting aliens gets to relive the same day over and over again, the day restarting every time he dies.",
											  "2014", "Action,Drama,Thriller;Adventure", 370000000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddPersonVertex = createPersonVertex("nm0000129", "Tom Cruise", "In 1976, if you had told fourteen-year-old Franciscan seminary student Thomas Cruise Mapother IV that one day in the not too distant future he would be Tom Cruise, one of the top 100 movie stars of all time, he would have probably grinned and told you that his ambition was to join the priesthood. Nonetheless, this sensitive, deeply religious youngster who was born in 1962 in Syracuse, New York, was destined to become one of the highest paid and most sought after actors in screen history. Tom is the only son (among four children) of nomadic parents, Mary Lee (Pfeiffer), a special education teacher, and Thomas Cruise Mapother III, an electrical engineer. His parents were both from Louisville, Kentucky, and he has German, Irish, and English ancestry. Young Tom spent his boyhood always on the move, and by the time he was 14 he had attended 15 different schools in the U.S. and Canada. He finally settled in Glen Ridge, New Jersey with his mother and her new husband. While in high school, Tom wanted to become a priest but pretty soon he developed an interest in acting and abandoned his plans of becoming a priest, dropped out of school, and at age 18 headed for New York and a possible acting career. The next 15 years of his life are the stuff of legends. He made his film debut with a small part in Endless Love (1981) and from the outset exhibited an undeniable box office appeal to both male and female audiences.",
												"1962", "actor,producer,soundtrack", true, 67, "Green");
			ddRoleEdge = createRoleEdge("pr000031", "Actor", "2013", "Co-Lead", "Cage");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm1289434", "Emily Blunt", "Emily Olivia Leah Blunt is a British actress known for her roles in The Devil Wears Prada (2006), The Young Victoria (2009), Edge of Tomorrow (2014), and The Girl on the Train (2016), among many others. Blunt was born on February 23, 1983, in Roehampton, South West London, England, the second of four children in the family of Joanna Mackie, a former actress and teacher, and Oliver Simon Peter Blunt, a barrister. Her grandfather was Major General Peter Blunt, and her uncle is MP Crispin Blunt. Emily received a rigorous education at Ibstock Place School, a co-ed private school at Roehampton. However, young Emily Blunt had a stammer, since she was a kid of 8. Her mother took her to relaxation classes, which did not do anything. She reached a turning point at 12, when a teacher cleverly asked her to play a character with a different voice and said, \"I really believe in you\". Blunt ended up using a northern accent, and it did the trick, her stammer disappeared.",
												"1983", "actress,soundtrack,miscellaneous", false, 67, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000032", "Actor", "2013", "Co-Lead", "Rita");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000200", "Bill Paxton", "Bill Paxton was born on May 17, 1955 in Fort Worth, Texas. He was the son of Mary Lou (Gray) and John Lane Paxton, a businessman and actor (as John Paxton). Bill moved to Los Angeles, California at age eighteen, where he found work in the film industry as a set dresser for Roger Corman's New World Pictures. He made his film debut in the Corman film Crazy Mama (1975), directed by Jonathan Demme. Moving to New York, Paxton studied acting under Stella Adler at New York University. After landing a small role in Stripes (1981), he found steady work in low-budget films and television. He also directed, wrote and produced award-winning short films including Barnes & Barnes: Fish Heads (1980), which aired on Saturday Night Live (1975). His first appearance in a James Cameron film was a small role in The Terminator (1984), followed by his very memorable performance as Private Hudson in Aliens (1986) and as the nomadic vampire Severen in Kathryn Bigelow's Near Dark (1987). Bill also appeared in John Hughes' Weird Science (1985), as Wyatt Donnelly's sadistic older brother Chet. Although he continued to work steadily in film and television, his big break did not come until his lead role in the critically acclaimed film-noir One False Move (1992). This quickly led to strong supporting roles as Wyatt Earp's naive younger brother Morgan in Tombstone (1993) and as Fred Haise, one of the three astronauts, in Apollo 13 (1995), as well as in James Cameron's offering True Lies (1994).",
												"1955", "actor,producer,writer", true, 72, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000033", "Actor", "2013", "Supporting", "Master Sergeant Farell");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			ddPersonVertexSaved = ddPersonVertex;

			ddPersonVertex = createPersonVertex("nm0065100", "Christophe Beck", "Christophe Beck was born in 1972 in Montreal, Quebec, Canada as Jean-Christophe Beck. He is known for his work on Frozen (2013), Ant-Man (2015) and The Muppets (2011).",
												"1972", "composer,music_department,soundtrack", true, 69, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000034", "Composer", "2013", "Lead", "Composer");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0499544", "Pitch Perfect", "Beca, a freshman at Barden University, is cajoled into joining The Bellas, her school's all-girls singing group. Injecting some much needed energy into their repertoire, The Bellas take on their male rivals in a campus competition.",
											  "2012", "Comedy,Musical", 116044347.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000035", "Composer", "2011", "Lead", "Composer");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0447695", "Anna Kendrick", "Anna Kendrick was born in Portland, Maine, to Janice (Cooke), an accountant, and William Kendrick, a teacher. She has an older brother, Michael Cooke Kendrick, who has also acted. She is of English, Irish, and Scottish descent. For her role as \"Dinah\" in \"High Society\" on Broadway, Anna Kendrick was nominated for a Tony Award (second youngest ever), a Drama Desk Award, and a Fany Award (best actress featured in a musical). Her spectacular performance landed her the Drama League and Theatre World Award.",
												"1985", "actress,soundtrack,producer", false, 62, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000036", "Actor", "2011", "Lead", "Beca");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm2799457", "Skylar Astin", "Skylar Astin was born Skylar Astin Lipstein in New York and grew up in Rockland County, part of the NY Metropolitan Area. He is the son of Meryl and Barry Lipstein, a garment industry executive. Astin has two brothers, Jace and Milan, and a sister, Brielle. He attended Clarkstown High School North. After graduation he attended New York University, as a student in the Tisch School of the Arts. He took a leave of absence to join the original cast of Spring Awakening as Georg on Broadway. He was in the show for close to a year before moving to LA and starting his acting career in movies and TV shows.",
												"1987", "actor,soundtrack", true, 69, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000037", "Actor", "2011", "Supporting", "Jesse");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0811242", "Brittany Snow", "Brittany Anne Snow (born March 9, 1986) is an American actress and singer. She began her career as Susan \"Daisy\" Lemay on the CBS series Guiding Light (1952) for which she won a Young Artist Award for Best Young Actress and was nominated for two other Young Artist Awards and a Soap Opera Digest Award. She then played the protagonist Meg Pryor on the NBC series American Dreams (2002) for which she was nominated for a Young Artist Award and three Teen Choice Awards.",
												"1986", "actress,soundtrack,producer", false, 65, "Green");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000038", "Actor", "2011", "Supporting", "Chloe");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm2319871", "Anna Camp", "Anna Camp grew up in South Carolina and is the daughter of Dee and Thomas Camp. Anna graduated from the University of North Carolina School of the Arts in 2004. She then moved to New York City and has appeared in films and television shows since 2007. From 2009 to 2014, she starred in the series True Blood (2008) as Sarah Newlin. She had a main role in The Mindy Project (2012) from 2012 to 2013. In 2011, Camp appeared in the film The Help (2011). She starred in Pitch Perfect (2012) and Pitch Perfect 2 (2015), playing the character of Aubrey Posen. She was married to Michael Mosley from 2010 to 2013 and married Skylar Astin in 2016.",
												"1982", "actress,soundtrack,producer", false, 68, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000039", "Actor", "2011", "Supporting", "Aubrey");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0112384", "Apollo 13", "NASA must devise a strategy to return Apollo 13 to Earth safely after the spacecraft undergoes massive internal damage putting the lives of the three astronauts on board in jeopardy.",
											  "1995", "Adventure,Drama,History", 335802271.0);
			dataGraph.addVertex(ddMovieVertex);

			ddPersonVertex = createPersonVertex("nm0000165", "Ron Howard", "Academy Award-winning filmmaker Ron Howard is one of this generation's most popular directors. From the critically acclaimed dramas A Beautiful Mind (2001) and Apollo 13 (1995) to the hit comedies Parenthood (1989) and Splash (1984), he has created some of Hollywood's most memorable films.",
												"1954", "actor,producer,director", true, 69, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000040", "Director", "1994", "Director", "Ron Howard");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000158", "Tom Hanks", "Thomas Jeffrey Hanks was born in Concord, California, to Janet Marylyn (Frager), a hospital worker, and Amos Mefford Hanks, an itinerant cook. His mother's family, originally surnamed \"Fraga\", was entirely Portuguese, while his father was of mostly English ancestry. Tom grew up in what he has called a \"fractured\" family. He moved around a great deal after his parents' divorce, living with a succession of step-families. No problems, no alcoholism - just a confused childhood. He has no acting experience in college and credits the fact that he could not get cast in a college play with actually starting his career. He went downtown, and auditioned for a community theater play, was invited by the director of that play to go to Cleveland, and there his acting career started.",
												"1956", "producer,actor,soundtrack", true, 72, "Green");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000041", "Actor", "1994", "Co-Lead", "Jim Lovell");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000102", "Kevin Bacon", "Kevin's early training as an actor came from The Manning Street. His debut as the strict Chip Diller in National Lampoon's Animal House (1978) almost seems like an inside joke, but he managed to escape almost unnoticed from that role. Diner (1982) became the turning point after a couple of television series and a number of less-than-memorable movie roles. In a cast of soon-to-be stars, he more than held his end up, and we saw a glimpse of the real lunatic image of The Bacon. He also starred in Footloose (1984), She's Having a Baby (1988), Tremors (1990) with Fred Ward, Flatliners (1990), and Apollo 13 (1995).",
												"1958", "actor,producer,soundtrack", true, 68, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000042", "Actor", "1994", "Co-Lead", "Jack Swigert");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000438", "Ed Harris", "Ed Harris was born in Tenafly, New Jersey, to Margaret (Sholl), a travel agent, and Robert Lee Harris, a bookstore worker who also sang professionally. Both of his parents were originally from Oklahoma. Harris grew up as the middle child. After graduating high school, he attended New York's Columbia University, where he played football. After viewing local theater productions, Harris took a sudden interest in acting. He left Columbia, headed to Oklahoma, where his parents were living, and enrolled in the University of Oklahoma's theater department. After graduation, he moved to Los Angeles to find work. He started acting in theater and television guest spots. Harris landed his first leading role in a film in cult-favorite George A. Romero's Knightriders (1981). Two years later, he got his first taste of critical acclaim, playing astronaut John Glenn in The Right Stuff (1983). Also that year, he made his New York stage debut in Sam Shepard's \"Fool for Love\", a performance that earned him an Obie for Outstanding Actor. Harris' career gathered momentum after that. In 2000, he made his debut as a director in the Oscar-winning film Pollock (2000).",
												"1950", "actor,producer,soundtrack", true, 70, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000043", "Actor", "1994", "Supporting", "Gene Kranz");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000599", "Kathleen Quinlan", "Kathleen Quinlan was born in Pasadena, California, the only child of Josephine (Zachry), a military-supply supervisor, and Robert Quinlan, a television sports director. She grew up in Mill Valley, Ca, and got her break in acting when George Lucas came to her high school to cast for his movie American Graffiti (1973). She followed up her one-line role four years later with Lifeguard (1976), and then several roles in the late 1970s and 1980s. Her breakthrough performance came in 1977, as Deborah in I Never Promised You a Rose Garden (1977). She was nominated for an Academy Award in 1995 for Apollo 13 (1995). She starred in the TV series Family Law (1999), but her contract stipulated that she could not work later than 6 pm, so she could be home with her husband Bruce Abbott, son [error] (b. October 17, 1990), and stepson Dalton Abbott (b. October 4, 1989). She currently works in television and film.",
												"1954", "actress,director,producer", false, 65, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000044", "Actor", "1994", "Supporting", "Marilyn Lovell");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = ddPersonVertexSaved;
			ddRoleEdge = createRoleEdge("pr000045", "Actor", "1994", "Co-Lead", "Fred Haise");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0117998", "Twister", "Bill and Jo Harding, advanced storm chasers on the brink of divorce, must join together to create an advanced weather alert system by putting themselves in the cross-hairs of extremely violent tornadoes.",
											  "1996", "Action,Adventure,Thriller", 495700000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000046", "Actor", "1995", "Co-Lead", "Bill Harding");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000166", "Helen Hunt", "Born on June 15, 1963, in Culver City, California, Helen Hunt earned her first screen credit with the 1973 TV movie Pioneer Woman. She later appeared in the acclaimed 1980s medical drama St. Elsewhere, before taking on the role of Jamie Buchman in the hit '90s sitcom Mad About You, for which she won four Emmys. Hunt went on to star in the box-office hits Twister, What Women Want and Cast Away, and won a Best Actress Academy Award for As Good as it Gets. She later earned another Oscar nomination for her performance in The Sessions, and directed the feature films Then She Found Me and Ride.",
												"1963", "actress,producer,director", false, 67, "Light Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000047", "Actor", "1995", "Co-Lead", "Dr. Jo Harding");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000415", "Jami Gertz", "Jami Gertz was born on October 28, 1965 in Chicago, Illinois, USA. She is an actress and producer, known for Twister (1996), The Lost Boys (1987) and Still Standing (2002). She has been married to Antony Ressler since June 16, 1989. They have three children.",
												"1965", "actress,producer,director", false, 65, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000048", "Actor", "1995", "Supporting", "Dr. Melissa Reeves");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000341", "Michael Crichton", "Michael Crichton was born in Chicago, Illinois, but grew up in Roslyn, New York. His father was a journalist and encouraged him to write and to type. Michael gave up studying English at Harvard University, having become disillusioned with the teaching standards--the final straw came when he submitted an essay by George Orwell that was given a \"B-.\" After giving up English and spending a year in Europe, Michael returned to Boston, Massachusetts, and attended Havard Medical School to train as a doctor. Several times, he was persuaded not to quit the course but did so after qualifying in 1969.",
												"1942", "writer,producer,director", true, 81, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000049", "Writer", "1995", "Writer", "Michael Crichton");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000957", "Jan de Bont", "Jan de Bont was born in the Netherlands to a Roman Catholic Dutch family on the 22 of October 1943. He has always had a creative mind and good mentality for camera techniques and soon got into film as a popular cinematographer. He worked on a huge number of films before finding himself on the production of the film Speed (1994), which became his first as a director. The film was a success and took him onto the next set for Twister (1996), which he also directed. But then the total flops started coming his way: firstly, Speed 2: Cruise Control (1997), which he wrote and directed but without the company of Keanu Reeves. He also directed the star-packed The Haunting (1999) but that also failed him at the Box Office.",
												"1942", "writer,producer,director", true, 72, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000050", "Director", "1995", "Director", "Jan de Bont");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000450", "Philip Seymour Hoffman", "Film and stage actor and theater director Philip Seymour Hoffman was born in the Rochester, New York, suburb of Fairport on July 23, 1967. He was the son of Marilyn (Loucks), a lawyer and judge, and Gordon Stowell Hoffman, a Xerox employee, and was mostly of German, Irish, English and Dutch ancestry. After becoming involved in high school theatrics, he attended New York University's Tisch School of the Arts, graduating with a B.F.A. degree in Drama in 1989. He made his feature film debut in the indie production Triple Bogey on a Par Five Hole (1991) as Phil Hoffman, and his first role in a major release came the next year in My New Gun (1992). While he had supporting roles in some other major productions like Scent of a Woman (1992) and Twister (1996), his breakthrough role came in Paul Thomas Anderson's Boogie Nights (1997).",
												"1967", "actor,producer,soundtrack", true, 69, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000051", "Actor", "1995", "Supporting", "Dustin Davis");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0090605", "Aliens", "Fifty-seven years after surviving an apocalyptic attack aboard her space vessel by merciless space creatures, Officer Ripley awakens from hyper-sleep and tries to warn anyone who will listen about the predators.",
											  "1979", "Action,Adventure,Sci-Fi", 184694992.0);
			dataGraph.addVertex(ddMovieVertex);

			ddPersonVertex = ddPersonVertexSaved;
			ddRoleEdge = createRoleEdge("pr000052", "Actor", "1978", "Supporting", "Private Hudson");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000116", "James Cameron", "ames Francis Cameron was born on August 16, 1954 in Kapuskasing, Ontario, Canada. He moved to the United States in 1971. The son of an engineer, he majored in physics at California State University before switching to English, and eventually dropping out. He then drove a truck to support his screenwriting ambition. He landed his first professional film job as art director, miniature-set builder, and process-projection supervisor on Roger Corman's Battle Beyond the Stars (1980) and had his first experience as a director with a two week stint on Piranha II: The Spawning (1981) before being fired.",
												"1954", "writer,producer,director", true, 73, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000053", "Director", "1978", "Director", "James Cameron");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			if (anIsMultiEdge)
			{
				ddRoleEdge = createRoleEdge("pr000054", "Writer", "1977", "Writer", "Writer");
				dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			}

			ddPersonVertex = createPersonVertex("nm0000244", "Sigourney Weaver", "Sigourney Weaver was born Susan Alexandra Weaver, on October 8, 1949, in Leroy Hospital in New York City. Her father, TV producer Sylvester L. Weaver Jr., originally wanted to name her Flavia, because of his passion for Roman history (he had already named her elder brother Trajan). Her mother, Elizabeth Inglis, was a British actress who had sacrificed her career for a family. Sigourney grew up in a virtual bubble of guiltless bliss, being taken care by nannies and maids. By 1959, the Weavers had resided in 30 different households. In 1961, Sigourney began attending the Brearly Girls Academy, but her mother moved her to another New York private school, Chapin. Sigourney was quite a bit taller than most of her other classmates (at the age of 13, she was already 5' 10\"), resulting in her constantly being laughed at and picked on; in order to gain their acceptance, she took on the role of class clown.",
												"1949", "actress,soundtrack,producer", false, 73, "Green");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000055", "Actor", "1978", "Lead", "Ripley");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000448", "Lance Henriksen", "An intense, versatile actor as adept at playing clean-cut FBI agents as he is psychotic motorcycle-gang leaders, who can go from portraying soulless, murderous vampires to burned-out, world-weary homicide detectives, Lance Henriksen has starred in a variety of films that have allowed him to stretch his talents just about as far as an actor could possibly hope. He played \"Awful Knoffel\" in the TNT original movie Evel Knievel (2004), directed by John Badham and executive produced by Mel Gibson. Henriksen portrayed \"Awful Knoffel\" in this project based on the life of the famed daredevil, played by George Eads. Henriksen starred for three seasons (1996-1999) on Millennium (1996), Fox-TV's critically acclaimed series created by Chris Carter (The X-Files (1993)). His performance as Frank Black, a retired FBI agent who has the ability to get inside the minds of killers, landed him three consecutive Golden Globe nominations for \"Best Performance by a Lead Actor in a Drama Series\" and a People's Choice Award nomination for \"Favorite New TV Male Star\".",
												"1940", "actor,miscellaneous", true, 70, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000056", "Actor", "1978", "Supporting", "Bishop");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0001663", "Paul Reiser", "As a seasoned actor, writer, producer, and stand-up comedian, Paul Reiser continues to add to his list of accomplishments. In addition to co-creating and starring on the critically-acclaimed NBC series, Mad About You (1992), which garnered him Emmy, Golden Globe, American Comedy Award and Screen Actors Guild nominations for Best Actor in a Comedy Series, his successes also include his book, \"Couplehood\", which sold over two million copies and reached the number one spot on \"The New York Times\" best-seller list, and \"Babyhood\", his follow-up book, which features his trademark humorous take on the adventures of being a first-time father, which also made \"The New York Times\" best-seller list. He also wrote follow-up bestseller Familyhood.",
												"1956", "actor,writer,producer", true, 70, "Brown");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000057", "Actor", "1978", "Supporting", "Burke");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0001021", "Veronica Cartwright", "Born in Bristol, England, Veronica is the older sister of the popular child actress Angela Cartwright. In her early career, Veronica was cast in a number of popular movies such as William Wyler's The Children's Hour (1961), Spencer's Mountain (1963) and Alfred Hitchcock's The Birds (1963). As such, she was cast as \"Jemima Boone\" in the popular television series Daniel Boone (1964), which ran from 1964 to 66. Her career after \"Daniel Boone\" may have been influenced by Hitchcock, since she appeared in both the remake of Invasion of the Body Snatchers (1978) and the horror classic Alien (1979). On television, she appeared twice as Lumpy's younger sister, \"Violet Rutherford\" and once as \"Peggy MacIntosh\" on Leave It to Beaver (1957) and had a small role in the television movie Still the Beaver (1983).",
												"1949", "actress,soundtrack", false, 66, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000058", "Actor", "1978", "Supporting", "Lambert");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
		}
		catch (Exception e)
		{
			System.err.printf("Exception: %s%n", e.getMessage());
			dataGraph = null;
		}

		return dataGraph;
	}

	public DataGraph createMatchedMovieGraph(boolean anIsMultiEdge)
	{
		DataGraph dataGraph = new DataGraph("Matched " + GRAPH_DB_NAME, Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);
		try
		{
			DataDoc ddMovieVertex = createMovieVertex("tt0068646", "The Godfather", "An organized crime dynasty's aging patriarch transfers control of his clandestine empire to his reluctant son.",
													  "1972", "Crime,Drama", 4309000000.0);
			dataGraph.addVertex(ddMovieVertex);

			DataDoc ddPersonVertex = createPersonVertex("nm0000199", "Al Pacino", "Alfredo James \"Al\" 'Pacino established himself as a film actor during one of cinema's most vibrant decades, the 1970s, and has become an enduring and iconic figure in the world of American movies. He was born April 25, 1940 in Manhattan, New York City, to Italian-American parents, Rose (nee Gerardi) and Sal Pacino.",
														"1940", "actor,producer,soundtrack", true, 68, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			DataDoc ddRoleEdge = createRoleEdge("pr000002", "Actor", "1971", "Supporting", "Michael Corleone");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000380", "Robert Duvall", "Veteran actor and director Robert Selden Duvall was born on January 5, 1931, in San Diego, CA, to Mildred Virginia (Hart), an amateur actress, and William Howard Duvall, a career military officer who later became an admiral. Duvall majored in drama at Principia College (Elsah, IL), then served a two-year hitch in the army after graduating in 1953. He began attending The Neighborhood Playhouse School of the Theatre In New York City on the G.I. Bill in 1955, studying under Sanford Meisner along with Dustin Hoffman, with whom Duvall shared an apartment. Both were close to another struggling young actor named Gene Hackman. Meisner cast Duvall in the play \"The Midnight Caller\" by Horton Foote, a link that would prove critical to his career, as it was Foote who recommended Duvall to play the mentally disabled \"Boo Radley\" in To Kill a Mockingbird (1962). This was his first \"major\" role since his 1956 motion picture debut as an MP in Somebody Up There Likes Me (1956), starring Paul Newman.",
												"1931", "actor,producer,soundtrack", true, 69, "Black");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000006", "Actor", "1971", "Supporting", "Tom Hagen");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddMovieVertex = createMovieVertex("tt0316356", "Open Range", "A former gunslinger is forced to take up arms again when he and his cattle crew are threatened by a corrupt lawman.",
											  "2003", "Action,Drama,Romance", 68300000.0);
			dataGraph.addVertex(ddMovieVertex);
			ddRoleEdge = createRoleEdge("pr000009", "Actor", "2002", "Co-Lead", "Boss Spearman");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);

			ddPersonVertex = createPersonVertex("nm0000126", "Kevin Costner", "Kevin Michael Costner was born on January 18, 1955 in Lynwood, California, the third child of Bill Costner, a ditch digger and ultimately an electric line servicer for Southern California Edison, and Sharon Costner (née Tedrick), a welfare worker. His older brother, Dan, was born in 1950. A middle brother died at birth in 1953. His father's job required him to move regularly, which caused Kevin to feel like an Army kid, always the new kid at school, which led to him being a daydreamer. As a teen, he sang in the Baptist church choir, wrote poetry, and took writing classes. At 18, he built his own canoe and paddled his way down the rivers that Lewis & Clark followed to the Pacific. Despite his present height, he was only 5'2\" when he graduated high school. Nonetheless, he still managed to be a basketball, football and baseball star. In 1973, he enrolled at California State University at Fullerton, where he majored in business. During that period, Kevin decided to take acting lessons five nights a week. He graduated with a business degree in 1978 and married his college sweetheart, Cindy Costner. He initially took a marketing job in Orange County. Everything changed when he accidentally met Richard Burton on a flight from Mexico. Burton advised him to go completely after acting if that is what he wanted. He quit his job and moved to Hollywood soon after. He drove a truck, worked on a deep sea fishing boat, and gave bus tours to stars' homes before finally making his own way into the films.",
												"1955", "actor,producer,soundtrack", true, 73, "Blue");
			dataGraph.addVertex(ddPersonVertex);
			ddRoleEdge = createRoleEdge("pr000010", "Actor", "2002", "Co-Lead", "Charley Waite");
			dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			if (anIsMultiEdge)
			{
				ddRoleEdge = createRoleEdge("pr000011", "Director", "2002", "Lead", "Director");
				dataGraph.addEdge(ddPersonVertex, ddMovieVertex, ddRoleEdge);
			}
		}
		catch (Exception e)
		{
			System.err.printf("Exception: %s%n", e.getMessage());
			dataGraph = null;
		}

		return dataGraph;
	}
}
