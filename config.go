package gdsb

/*
	Copyright 2018 Rewati Raman rewati.raman@gmail.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
	limitations under the License.
*/
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
			log.Panic("Resource dir evironement is empty... Please set GDSB_RESOURCE env or place config file in current directory")
			return resourceDir, errors.New("Resource dir evironement is empty... Please set GDSB_RESOURCE env or place config file in current directory")
		}
		return resourceDir, nil
	}
	return "./", nil
}
