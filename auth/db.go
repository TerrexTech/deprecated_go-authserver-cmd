package auth

import (
	"log"

	ec "github.com/TerrexTech/go-authserver-cmd/errors"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

// DBIConfig is the configuration for the authDB.
type DBIConfig struct {
	Hosts               []string
	Username            string
	Password            string
	TimeoutMilliseconds uint32
	Database            string
	Collection          string
}

// DBI is the Database-interface for authentication.
// This fetches/writes data to/from database for auth-actions such as
// login, registeration etc.
type DBI interface {
	Collection() *mongo.Collection
	GetMaxVersion() (int64, error)
	Register(user *User) (*User, error, int16)
}

// DB is the implementation for dbI.
// DBI is the Database-interface for authentication.
// It fetches/writes data to/from database for auth-actions such as
// login, registeration etc.
type DB struct {
	collection *mongo.Collection
}

// EnsureAuthDB exists ensures that the required Database and Collection exists before
// auth-operations can be done on them. It creates Database/Collection if they don't exist.
func EnsureAuthDB(dbConfig DBIConfig) (*DB, error) {
	config := mongo.ClientConfig{
		Hosts:               dbConfig.Hosts,
		Username:            dbConfig.Username,
		Password:            dbConfig.Password,
		TimeoutMilliseconds: dbConfig.TimeoutMilliseconds,
	}

	client, err := mongo.NewClient(config)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}

	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 5000,
	}

	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name: "username",
				},
			},
			IsUnique: true,
			Name:     "username_index",
		},
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name:        "version",
					IsDescOrder: true,
				},
			},
			IsUnique: true,
			Name:     "version_index",
		},
	}

	// ====> Create New Collection
	collConfig := &mongo.Collection{
		Connection:   conn,
		Database:     dbConfig.Database,
		Name:         dbConfig.Collection,
		SchemaStruct: &User{},
		Indexes:      indexConfigs,
	}
	c, err := mongo.EnsureCollection(collConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}
	return &DB{
		collection: c,
	}, nil
}

// Register inserts the provided User into database.
func (d *DB) Register(user *User) (*User, error, int16) {
	user.ID = objectid.New()
	uid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Registration: Error generating UUID")
		return nil, err, ec.InternalError
	}
	user.UUID = uid

	hashedPass, err := bcrypt.GenerateFromPassword([]byte(user.Password), 10)
	log.Printf(`"%s"`, user.Password)
	if err != nil {
		err = errors.Wrap(err, "Registration: Error creating Hash for password")
		return nil, err, ec.InternalError
	}
	user.Password = string(hashedPass)

	_, err = d.collection.InsertOne(user)
	log.Println("ggggggggggggggggggggg")
	if err != nil {
		err = errors.Wrap(err, "Registration: Error inserting user into Database")
		return nil, err, ec.UsernameExistsError
	}
	// Don't send hashed-password to any other service
	user.Password = ""
	return user, nil, 0
}

// Collection returns the currrent MongoDB collection being used for user-auth operations.
func (d *DB) Collection() *mongo.Collection {
	return d.collection
}

// GetMaxVersion returns the maximum event-hydration version for the Aggregate.
// This version is updated everytime new events are processed.
func (d *DB) GetMaxVersion() (int64, error) {
	findResult, err := d.collection.FindOne(
		map[string]interface{}{
			"version": map[string]int{
				"$gt": 0,
			},
		},
		findopt.Sort(
			map[string]interface{}{
				"version": -1,
			},
		),
	)
	if err != nil {
		err = errors.Wrap(err, "Error fetching max version")
		log.Println(err)
		log.Println("Version will be assumed to be 1 (new Aggregate)")
		return 1, nil
	}

	user, ok := findResult.(*User)
	if !ok {
		err = errors.New("GetMaxVersion Error: Unable to convert result to User")
		return -1, err
	}
	return user.Version, nil
}
