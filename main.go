package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)


type User struct {
	Id      string `dnamodbav:"id"`
	Name    string `dnamodbav:"name"`
	Address string `dnamodbav:"address"`
}

type TableBasics struct {
	DynamoDbClient *dynamodb.Client `json:"dynamodbclient"`
	TableName      string           `json:"tablename"`
}

type Name struct {
	Name string `json:"name"`
}

type IdStruct struct{
	Id	string	`dnamodbav:"id"`
}

var basics TableBasics
var consumer sarama.Consumer
var topic string

func createLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
	config.WithRegion("localhost"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://10.225.51.152:8000"}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID: "h0z05k", SecretAccessKey: "lxoe6", SessionToken: "dummy",
				},
			}),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func buildCreateTableInput(tableName string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("Id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("Id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
	}
}

func tableExists(t string) bool{

	tables, err := basics.DynamoDbClient.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Fatal("ListTables failed", err)
	}
	for _, n := range tables.TableNames {
		if n == t {
			fmt.Println("Table already exists")
			return true
		}
	}
	fmt.Println("Table does not exists")
	return false
}

func createTableIfNotExists(basics TableBasics) (*types.TableDescription, error) {
	table, err := basics.DynamoDbClient.CreateTable(context.TODO(), buildCreateTableInput(basics.TableName))
	if err != nil {
		log.Fatal("CreateTable failed", err)
	}
	if err != nil {
		log.Printf("Couldn't create table %v. Here's why: %v\n", basics.TableName, err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(basics.DynamoDbClient)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(basics.TableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
	}
	return table.TableDescription, err
}

func createConnectionHandler() {
	svc := createLocalClient()
	fmt.Println("SVC = ",svc)
	if svc == nil {
		fmt.Println("Local connection not established")
		return
	}
	basics.DynamoDbClient = svc
	basics.TableName="users"
	fmt.Println("Connection Created")
}



func createTableHandler(){
	table, err := createTableIfNotExists(basics)
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Println("Table = ", table)
	fmt.Println("Table Created Successfully")
}

func deleteTableHandler(w http.ResponseWriter, r *http.Request) {
	var tname Name
	err := json.NewDecoder(r.Body).Decode(&tname)
	if err != nil {
		fmt.Println(err)
		return
	}
	out, err := basics.DynamoDbClient.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String(tname.Name),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	io.WriteString(w, "Table Deleted Successfully\n")
}

func (t IdStruct) GetKey() map[string]types.AttributeValue {
	id, err := attributevalue.Marshal(t.Id)
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{"Id": id,}
}

func deleteUserHandler(data string) {
	var id =IdStruct{
		Id:data,
	}
	// err := json.Unmarshal([]byte(data),&id)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	out, err := basics.DynamoDbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: aws.String("users"), Key:id.GetKey(),
	})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(out)
	fmt.Println("User Deleted Successfully")
}
func getUserHandler(data string){
	var id =IdStruct{
		Id:data,
	}
	// err := json.Unmarshal([]byte(data),&Username)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	out, err := basics.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("users"), Key:id.GetKey(),
	})
	if err != nil {
		panic(err)
	}
	resp,_ := json.MarshalIndent(out.Item," ","")
	fmt.Println(string(resp))
	fmt.Println("Got user successully")
}

func scanTableHandlerHandler(){
	out, err := basics.DynamoDbClient.Scan(context.TODO(), &dynamodb.ScanInput{
        TableName: aws.String(basics.TableName),
    })
    if err != nil {
        panic(err)
    }
	var arr []string
	for _,ele := range out.Items{
		resp,_ := json.MarshalIndent(ele," ","")
		arr = append(arr, string(resp))
	}
	
	fmt.Println(arr)
}


func addUserHandler(data string) {
	var newUser User
	err := json.Unmarshal([]byte(data),&newUser)
	if err!=nil{
		fmt.Println("Error : ",err)
	}

	newUser.Id = uuid.New().String()

	// fmt.Println(newUser)

	item, err := attributevalue.MarshalMap(newUser)
	if err != nil {
		fmt.Println("Error : ",err)
	}
	// fmt.Println(item)
	_, err = basics.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(basics.TableName),
		Item:      item,
	})
	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
		// io.WriteString(w, "Add item Failed\n")
		return
	}
	// io.WriteString(w, "Item successfully added \n")
}


func main() {

	createConnectionHandler()
	
	if !tableExists("users"){
		createTableHandler()
	}

	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.RequiredAcks = sarama.WaitForAll

	brokers := []string{"localhost:9092"}

	topic = "userops"

	consumer, _= sarama.NewConsumer(brokers, config)

	defer func() { _ = consumer.Close() }()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalln("Failed to start Sarama partition consumer:", err)
	}

	for{
		select {
		case msg, ok := <-partitionConsumer.Messages():
		   if ok {
				arr := strings.Split(string(msg.Value),"?")
				fmt.Println(arr)

				switch arr[0]{
					case "root":
						fmt.Println("DB server running...")
					case "getalluser":
						scanTableHandlerHandler()
					case "adduser":
						addUserHandler(arr[1])
					case "getuser":
						getUserHandler(arr[1])
					case "deleteuser":
						deleteUserHandler(arr[1])
					default:
						fmt.Println("No such route")
				}


			//   fmt.Fprintf(os.Stdout, "%s  %d  %d  %s  %s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			//   partitionConsumer.(msg, "")   // mark message as processed
		   }
		case <-signals:
		   return
		}
	}


	// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	// 	io.WriteString(w, "DB server running...\n")
	// })
	// http.HandleFunc("/connect", createConnectionHandler)
	// http.HandleFunc("/table/exists", func(w http.ResponseWriter, r *http.Request) {
	// 	var tname Name
	// 	err := json.NewDecoder(r.Body).Decode(&tname)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return
	// 	}
	// 	n := tname.Name
	// 	tableExists(n, w, r)
	// })
	// http.HandleFunc("/table/create", createTableHandler)
	// http.HandleFunc("/table/delete", deleteTableHandler)
	// http.HandleFunc("/user/add", addUserHandler)
	// http.HandleFunc("/user/get", getUserHandler)
	// http.HandleFunc("/user/delete", deleteUserHandler)
	// http.HandleFunc("/user/scan",scanTableHandlerHandler)
	// http.ListenAndServe(":8040", nil)
}