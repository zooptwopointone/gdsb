package gdsb

import (
	"errors"
	"log"
	"os"

	"github.com/spf13/viper"
)

//Configuration holds Config interface to be loaded and config validator
type Configuration struct {
	Config  interface{}
	EnvName string
}

//LoadConfigurationsFromDir Load Configurations From Dir
func loadConfigurationsFromDir(resourcesDir string, configuration Configuration) error {
	v := viper.New()
	v.SetConfigName("appConfig")
	v.SetConfigType("yaml")
	v.AddConfigPath(resourcesDir)
	if err := v.ReadInConfig(); err != nil {
		return err
	}
	if err := v.Unmarshal(&configuration.Config); err != nil {
		return err
	}
	return nil
}

//LoadConfigurations loads configurations
func LoadConfigurations(c Configuration) error {
	resourceDir, _ := getResourceDir(c.EnvName)
	if err := loadConfigurationsFromDir(resourceDir, c); err != nil {
		log.Panicf("Error while loading Configuration. Error: %v", err)
		return err
	}
	return nil
}

func getResourceDir(env string) (string, error) {
	if _, err := os.Stat("./appConfig.yaml"); os.IsNotExist(err) {
		var resourceDir string
		if len(env) != 0 {
			resourceDir = os.Getenv(env)
		}
		if len(resourceDir) == 0 {
			log.Panic("Resource dir evironement is empty... Please set TINIIFY_RESOURCE env or place config file in current directory")
			return resourceDir, errors.New("Resource dir evironement is empty... Please set TINIIFY_RESOURCE env or place config file in current directory")
		}
		return resourceDir, nil
	}
	return "./", nil
}
