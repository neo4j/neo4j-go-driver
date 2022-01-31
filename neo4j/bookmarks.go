package neo4j

/*
Bookmarks is a holder for server-side bookmarks which are used for causally chaining sessions.

See also CombineBookmarks.

Note:
Will be changed from being a type alias to being a struct in 6.0. Please use BookmarksFromRawValues for construction
from raw values and BookmarksToRawValues for accessing the raw values.
*/
type Bookmarks = []string

/*
CombineBookmarks is a helper method to combine []Bookmarks into a single Bookmarks instance.

Let s1, s2, s3 be Session interfaces. You can easily causally chain the sessions like so:

	s4 := driver.NewSession(neo4j.SessionConfig{
		Bookmarks: neo4j.CombineBookmarks(s1.LastBookmarks(), s2.LastBookmarks(), s3.LastBookmarks()),
	})

The server will then make sure to execute all transactions in s4 after any that were already executed in s1, s2, or s3
at the time of calling LastBookmarks.
*/
func CombineBookmarks(bookmarks... Bookmarks) Bookmarks  {
	var lenSum int
	for _, b := range bookmarks {
		lenSum += len(b)
	}
	res := make([]string, lenSum)
	var i int
	for _, b := range bookmarks {
		i += copy(res[i:], b)
	}
	return res
}

/*
BookmarksToRawValues exposes the raw server-side bookmarks.

You should not need to use this method unless you want to serialize bookmarks.
See Session.LastBookmarks and CombineBookmarks for alternatives.
 */
func BookmarksToRawValues(bookmarks Bookmarks) []string {
	return bookmarks
}

/*
BookmarksFromRawValues creates Bookmarks from raw server-side bookmarks.

You should not need to use this method unless you want to de-serialize bookmarks.
See Session.LastBookmarks and CombineBookmarks for alternatives.
*/
func BookmarksFromRawValues(values... string) Bookmarks {
	return values
}
