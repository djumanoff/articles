package main

import (
	"database/sql"
	"encoding/json"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3" // Import go-sqlite3 library
	"log"
	"net/http"
	"os"
)

const dbFilePath = "./data.sqlite"

var schemaSQL = []string{`CREATE TABLE IF NOT EXISTS drivers (
  id integer PRIMARY KEY,
  driver_info varchar(255),
  rating_sum bigint,
  rating_count bigint
)`, `
CREATE TABLE IF NOT EXISTS driver_ratings (
  driver_id integer,
  user_id varchar(255),
  rating integer
)`}

var db *sql.DB

type Rating struct {
	UserID   string `json:"user_id"`
	DriverID string `json:"driver_id"`
	Rating   int    `json:"rating"`
}

type Driver struct {
	ID            string  `json:"id"`
	DriverInfo    string  `json:"driver_info"`
	AverageRating float64 `json:"avg_rating"`
}

func rate(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	driverId := params["driver_id"]
	dec := json.NewDecoder(r.Body)
	var rating Rating
	err := dec.Decode(&rating)
	if err != nil {
		panic(err)
	}
	err = createOrUpdateRating(driverId, rating.UserID, rating.Rating)
	if err != nil {
		panic(err)
	}
	w.WriteHeader(200)
}

func getDrivers(w http.ResponseWriter, r *http.Request) {
	list, err := getDriversList()
	if err != nil {
		panic(err)
	}
	d, err := json.Marshal(list)
	if err != nil {
		panic(err)
	}
	_, err = w.Write(d)
	if err != nil {
		panic(err)
	}
	w.WriteHeader(200)
}

func getDriverRatings(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	driverId := params["driver_id"]
	list, err := getDriverRatingsList(driverId)
	if err != nil {
		panic(err)
	}
	d, err := json.Marshal(list)
	if err != nil {
		panic(err)
	}
	_, err = w.Write(d)
	if err != nil {
		panic(err)
	}
	w.WriteHeader(200)
}

func createTables() {
	for _, q := range schemaSQL {
		statement, err := db.Prepare(q) // Prepare SQL Statement
		if err != nil {
			log.Fatal(err.Error())
		}
		statement.Exec()
	}
	for i := 1; i <= 30; i++ {
		query := `INSERT INTO drivers (id, driver_info, rating_sum, rating_count) VALUES (?, ?, 0, 0)`
		statement, err := db.Prepare(query) // Prepare statement.
		// This is good to avoid SQL injections
		if err != nil {
			log.Fatal(err.Error())
		}
		_, err = statement.Exec(i, "{}")
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func createOrUpdateRating(driverId, userId string, rating int) error {
	ratingObject, err := getRating(driverId, userId)
	if ratingObject == nil && err == nil {
		query := `INSERT INTO driver_ratings (driver_id, user_id, rating) VALUES (?, ?, ?)`
		statement, err := db.Prepare(query) // Prepare statement.
		// This is good to avoid SQL injections
		if err != nil {
			return err
		}
		_, err = statement.Exec(driverId, userId, rating)
		if err != nil {
			return err
		}
		query = `UPDATE drivers 
      SET rating_sum = rating_sum + ?, 
        rating_count = rating_count + 1 
      WHERE id = ?`
		statement, err = db.Prepare(query) // Prepare statement.
		// This is good to avoid SQL injections
		if err != nil {
			return err
		}
		_, err = statement.Exec(rating, driverId)
		if err != nil {
			return err
		}
	} else if ratingObject != nil {
		query := `UPDATE driver_ratings SET rating = ? WHERE driver_id = ? AND user_id = ?`
		statement, err := db.Prepare(query) // Prepare statement.
		// This is good to avoid SQL injections
		if err != nil {
			return err
		}
		_, err = statement.Exec(rating, driverId, userId)
		if err != nil {
			return err
		}
		query = `UPDATE drivers 
      SET rating_sum = rating_sum + ? 
      WHERE id = ?`
		statement, err = db.Prepare(query) // Prepare statement.
		// This is good to avoid SQL injections
		if err != nil {
			return err
		}
		_, err = statement.Exec(rating-ratingObject.Rating, driverId)
		if err != nil {
			return err
		}
	}
	return err
}

func getRating(driverId, userId string) (*Rating, error) {
	row, err := db.Query("SELECT rating FROM driver_ratings WHERE driver_id = ? AND user_id = ?", driverId, userId)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	for row.Next() { // Iterate and fetch the records from result cursor
		var rating int
		err = row.Scan(&rating)
		if err != nil {
			return nil, err
		}
		return &Rating{userId, driverId, rating}, nil
	}
	return nil, nil
}

func getDriversList() ([]Driver, error) {
	row, err := db.Query("SELECT r.id, r.driver_info, COALESCE(r.rating_sum/r.rating_count, 0) AS avg_rating FROM drivers r")
	if err != nil {
		return nil, err
	}
	defer row.Close()
	var list []Driver
	for row.Next() { // Iterate and fetch the records from result cursor
		var driver Driver
		err = row.Scan(&driver.ID, &driver.DriverInfo, &driver.AverageRating)
		if err != nil {
			return nil, err
		}
		list = append(list, driver)
	}
	return list, nil
}

func getDriverRatingsList(driverId string) ([]Rating, error) {
	row, err := db.Query("SELECT driver_id, user_id, rating FROM driver_ratings WHERE driver_id = ?", driverId)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	var list []Rating
	for row.Next() { // Iterate and fetch the records from result cursor
		var rating Rating
		err = row.Scan(&rating.DriverID, &rating.UserID, &rating.Rating)
		if err != nil {
			return nil, err
		}
		list = append(list, rating)
	}
	return list, nil
}

/*
main function
*/
func main() {
	os.Remove(dbFilePath)
	file, err := os.Create(dbFilePath)
	if err != nil {
		panic(err)
	}
	file.Close()
	db, _ = sql.Open("sqlite3", dbFilePath)
	defer db.Close()
	createTables()

	r := mux.NewRouter()
	r.HandleFunc("/drivers/{driver_id}/ratings", rate).Methods("POST")
	r.HandleFunc("/drivers", getDrivers).Methods("GET")
	r.HandleFunc("/drivers/{driver_id}/ratings", getDriverRatings).Methods("GET")

	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatal(err)
	}
}
