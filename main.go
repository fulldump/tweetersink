package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/fulldump/goconfig"

	"tweetersink/inceptiondb"
)

type Config struct {
	Inception inceptiondb.Config
	Twitter   Twitter
}

type Twitter struct {
	ApiKey       string
	ApiKeySecret string
	BearerToken  string
}

func main() {

	collectionName := "raw3"

	c := Config{
		// default config
	}
	goconfig.Read(&c)

	fmt.Println(c.Inception.DatabaseID)
	fmt.Println(c.Inception.Base)

	db := inceptiondb.NewClient(c.Inception)

	db.EnsureIndex(collectionName, &inceptiondb.IndexOptions{
		Name:   "username,tweet_id",
		Type:   "btree",
		Fields: []string{"username", "id"},
	})
	db.EnsureIndex(collectionName, &inceptiondb.IndexOptions{
		Name:   "followers_count,tweet_id",
		Type:   "btree",
		Fields: []string{"followers_count", "id"},
	})
	db.EnsureIndex(collectionName, &inceptiondb.IndexOptions{
		Name:   "tweet_count,tweet_id",
		Type:   "btree",
		Fields: []string{"tweet_count", "id"},
	})

	// reader, err := db.Find("tweets", inceptiondb.FindQuery{})
	// if err != nil {
	// 	panic(err)
	// }
	// io.Copy(os.Stdout, reader)

	// print config:
	// e := json.NewEncoder(os.Stdout)
	// e.SetIndent("", "  ")
	// e.Encode(c)

	// Experiments:
	//	getTweets(c.Twitter)
	// createRule(c.Twitter)
	// getRule(c.Twitter)
	// getTweets(c.Twitter)

	totalTweets := 0

	now := time.Now()

	go func() {
		for {
			fmt.Println("totalTweets", totalTweets, ", etps", 100*float64(totalTweets)/time.Since(now).Seconds())
			time.Sleep(1 * time.Second)
		}
	}()

	r, w := io.Pipe()

	e := json.NewEncoder(w)

	go getSampleStream(c.Twitter, func(t JSON) {
		totalTweets++

		info := struct {
			Data struct {
				ID        string `json:"id"`
				Lang      string `json:"lang"`
				CreatedAt string `json:"created_at"`
			} `json:"data"`
			Includes struct {
				Users []struct {
					ID            string `json:"id"`
					Username      string `json:"username"`
					PublicMetrics struct {
						FollowersCount int64 `json:"followers_count"`
						TweetCount     int64 `json:"tweet_count"`
					} `json:"public_metrics"`
				}
			} `json:"includes"`
		}{}

		json.Unmarshal(t, &info)

		/*
			data.id
			data.lang
			data.created_at
			includes.users.0.public_metrics.followers_count
			includes.users.0.public_metrics.tweet_count
		*/

		e.Encode(map[string]interface{}{
			"id":              info.Data.ID,
			"lang":            info.Data.Lang,
			"created_at":      info.Data.CreatedAt,
			"user_id":         info.Includes.Users[0].ID,
			"username":        info.Includes.Users[0].Username,
			"followers_count": info.Includes.Users[0].PublicMetrics.FollowersCount,
			"tweet_count":     info.Includes.Users[0].PublicMetrics.TweetCount,
			"original":        t,
		})
	})

	db.InsertStream(collectionName, r)

	// getTweetById(c.Twitter, "1610789920179732480")
}

func getTweets(c Twitter) {
	endpoint := "https://api.twitter.com/2/tweets/search/stream"
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+c.BearerToken)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.StatusCode)

	io.Copy(os.Stdout, res.Body)

	fmt.Println("")

}

func getTweetById(c Twitter, id string) {
	endpoint := "https://api.twitter.com/2/tweets/" + url.PathEscape(id)
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+c.BearerToken)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.StatusCode)

	io.Copy(os.Stdout, res.Body)

	res.Body.Close()

	fmt.Println("")
}

type JSON = json.RawMessage

const allTweetFields = "attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,edit_history_tweet_ids,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,possibly_sensitive,promoted_metrics,public_metrics,referenced_tweets,reply_settings,source,text,withheld"
const allUserFields = "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,verified_type,withheld"
const allPlaceFields = "contained_within,country,country_code,full_name,geo,id,name,place_type"
const allExpansions = "attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id,edit_history_tweet_ids"
const allMediaFields = "duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width"
const allPollFields = "duration_minutes,end_datetime,id,options,voting_status"

func getSampleStreamReader(c Twitter) io.ReadCloser {
	queryParams := url.Values{}
	queryParams.Set("expansions", allExpansions)
	queryParams.Set("tweet.fields", allTweetFields)
	queryParams.Set("user.fields", allUserFields)
	queryParams.Set("place.fields", allPlaceFields)
	queryParams.Set("media.fields", allMediaFields)
	queryParams.Set("poll.fields", allPollFields)

	endpoint := "https://api.twitter.com/2/tweets/sample/stream?" + queryParams.Encode()

	// fmt.Println("endpoint:", endpoint)

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+c.BearerToken)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	if res.StatusCode != http.StatusOK {
		io.Copy(os.Stdout, res.Body)
		return nil
	}

	return res.Body
}

func getSampleStream(c Twitter, callback func(t JSON)) {

	reader := getSampleStreamReader(c)
	defer reader.Close()

	d := json.NewDecoder(reader)

	for {
		t := JSON{}
		err := d.Decode(&t)
		if err != nil {
			panic(err)
		}
		callback(t)
	}
}

func getRule(c Twitter) {
	endpoint := "https://api.twitter.com/2/tweets/search/stream/rules"
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+c.BearerToken)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.StatusCode)

	io.Copy(os.Stdout, res.Body)

	fmt.Println("")
}

func createRule(c Twitter) interface{} {

	endpoint := "https://api.twitter.com/2/tweets/search/stream/rules"
	req, err := http.NewRequest("POST", endpoint, strings.NewReader(`
		{
			"add":[
				{"tag":"1","value":"Xavi"}
			]
		}
	`))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+c.BearerToken)
	req.Header.Set("Content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.StatusCode)

	io.Copy(os.Stdout, res.Body)

	return nil
}
