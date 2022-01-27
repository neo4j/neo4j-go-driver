package neo4j

/*
CombineBookmarks is a helper method to flatten a [][]string slice into a []sting slice.

Let s1, s2, s3 be Session interfaces. You can easily causally chain the sessions like so:

	s4 := driver.NewSession(neo4j.SessionConfig{
		Bookmarks: neo4j.CombineBookmarks(s1.LastBookmarks(), s2.LastBookmarks(), s3.LastBookmarks()),
	})

The server will then make sure to execute all transactions in s4 after any that were already executed in s1, s2, or s3
at the time of calling LastBookmarks.
*/
func CombineBookmarks(slices... []string) []string  {
	var lenSum int
	for _, s := range slices {
		lenSum += len(s)
	}
	res := make([]string, lenSum)
	var i int
	for _, s := range slices {
		i += copy(res[i:], s)
	}
	return res
}
