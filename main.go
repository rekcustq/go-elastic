package main

import(
	"context"
	"encoding/json"
	"fmt"

	"github.com/olivere/elastic"
)

type Account struct {
	Id			string	`json:"id"`
	Firstname	string	`json:"firstname"`
	Lastname	string	`json:"lastname"`
	Age			int64	`json:"age"`
	Gender		string	`json:"gender"`
	Balance		float64	`json:"balance"`
	Address		string	`json:"address"`
}

func GetESClient() (*elastic.Client, error) {

	client, err :=  elastic.NewClient(elastic.SetURL("http://localhost:9200"),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false))

	fmt.Println("ES initialized...")

	return client, err

}

func main() {
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}

	var accounts []Account

	searchSource := elastic.NewSearchSource()
	searchSource.Query(elastic.NewMatchQuery("age", "40"))

	queryStr, err1 := searchSource.Source()
	queryJs, err2 := json.Marshal(queryStr)

	if err1 != nil || err2 != nil {
		fmt.Println("[esclient][GetResponse]err during query marshal=", err1, err2)
	}
	fmt.Println("[esclient]Final ESQuery=\n", string(queryJs))

	searchService := esclient.Search().Index("bank").SearchSource(searchSource)
    
	searchResult, err := searchService.Do(ctx)
	fmt.Println(searchResult)
	if err != nil {
		fmt.Println("[ProductsES][GetPIds]Error=", err)
		return
	}

	for _, hit := range searchResult.Hits.Hits {
		var account Account
		err := json.Unmarshal(hit.Source, &account)
		if err != nil {
			fmt.Println("[Getting accounts][Unmarshal] Err=", err)
		}

		accounts = append(accounts, account)
	}

	if err != nil {
		fmt.Println("Fetching account fail: ", err)
	} else {
		for _, s := range accounts {
			fmt.Printf("account found FirstName: %s, LastName: %s, Age: %d, Score: %f \n", s.Firstname, s.Lastname, s.Age, s.Balance)
		}
	}
}