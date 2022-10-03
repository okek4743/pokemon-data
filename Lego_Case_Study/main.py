import json
import os
import pandas as pd
import requests


class PokemonData:
    """
    This class defines the functions to fufill the Pokemon Case Study
    """

    def __init__(self):
        return

    def check_json_file_exists(self, file_name):
        """check if the json file exists"""
        return os.path.exists(file_name)

    def get_data(self, *, json_cache: str, update: bool = False, url: str):
        """get the data from local cache if it already exists and if not,call the endpoint and create it"""
        if update:
            json_data = None
        else:
            try:
                with open(json_cache, "r") as json_file:
                    json_data = json.load(json_file)
                    print("data fetched from local cache")
            except (FileNotFoundError, json.JSONDecodeError) as e:
                print(f"Local cache not found ... ({e})")
                json_data = None

        if not json_data:
            print("Get new json data and create local cache")
            try:
                response = requests.get(url)
                json_data = response.json()
            except:
                print(f"There is a {response.status_code} error with your request")

            with open(json_cache, "w") as json_file:
                json.dump(json_data, json_file)

        return json_data

    def get_pokemon_endpoints(self):
        """gets the pokemon endpoints"""
        json_cache = "pokemon_data_folder/pokemon_endpoints_data.json"
        url = "https://pokeapi.co/api/v2/pokemon/?limit=1154"
        pokemon_endpoint_data = self.get_data(
            update=False, json_cache=json_cache, url=url
        )
        return pokemon_endpoint_data

    def get_pokemon_details(self):
        """gets the details of each pokemon"""
        # get pokemon_details_data
        if self.check_json_file_exists("pokemon_data_folder/pokemon_details.json"):
            print("reading the pokemon details from local cache")
            with open("pokemon_data_folder/pokemon_details.json", "r") as json_file:
                pokemon_details_list = json.load(json_file)
        else:
            print("requesting pokemon details from endpoints")
            dfs = []
            data = self.get_pokemon_endpoints()
            for result in data["results"]:
                response = requests.get(result["url"])
                pokemon_details_data = response.json()
                dfs.append(pokemon_details_data)
            print("writing pokemon details to file")

            with open("pokemon_data_folder/pokemon_details.json", "w") as json_file:
                json.dump(dfs, json_file, indent=4)
            json_file.close()
            pokemon_details_list = dfs
        return pokemon_details_list

    def normalize_pokemon_details(self):
        """flattens the pokemon json data"""
        dfs_normalize = []
        print("normalizing pokemon details")
        pokemon_details_list = self.get_pokemon_details()
        for pokemon_detail in pokemon_details_list:
            dfs_normalize.append((pd.json_normalize(pokemon_detail, max_level=1)))
        return dfs_normalize

    def get_pokemon_normalized_df(self):
        """gets the flattened json data in form of a Dataframe"""
        normalized_pokemon_details_data = self.normalize_pokemon_details()
        pokemon_df = normalized_pokemon_details_data[0]
        for other_df in normalized_pokemon_details_data[1:]:
            pokemon_df = pd.concat([pokemon_df, other_df])
        return pokemon_df

    def get_pokemon_game_indicies(self):
        """gets the pokemon game indicies"""
        print("get pokemon game indicies")
        pokemon_df = self.get_pokemon_normalized_df()
        pokemon_game_indices_list = []
        pokemon_game_indices = pokemon_df["game_indices"]
        for val in pokemon_game_indices:
            pokemon_game_indices_list.append(pd.json_normalize(val))
        return pokemon_game_indices_list

    def check_chosen_games_in_games(self, chosen_games: list, games: list):
        """checks to see if any of the chosen games(red,blue,leafgreen or white.) are present in the games"""
        return not set(chosen_games).isdisjoint(games)

    def check_pokemon_appear_in_games(self):
        """returns a boolean list showing if the chosen games are present in the games list for each pokemon"""
        pokemon_game_indicies = self.get_pokemon_game_indicies()
        print("Checking pokemon appeared in games")
        chosen_games = ["red", "blue", "leafgreen", "white"]
        pokemon_in_games = []

        for idx in range(0, len(pokemon_game_indicies)):
            try:
                if "version.name" in pokemon_game_indicies[idx]:
                    games = pokemon_game_indicies[idx]["version.name"]
                else:
                    games = []
            except:
                print("Out of index error")
            pokemon_in_games.append(
                self.check_chosen_games_in_games(chosen_games, games)
            )

        return pokemon_in_games

    def get_pokemon_data_in_chosen_games(self):
        """gets data for pokemon who appear in any of the chosen games(red,blue,leafgreen or white.)"""
        print("fetching pokemon details that appeared in the chosen games")
        pokemon_df = self.get_pokemon_normalized_df()
        pokemon_appear_in_games = self.check_pokemon_appear_in_games()
        pokemon_appear_in_games_df = pd.DataFrame(
            pokemon_appear_in_games, columns=["Appear_in_chosen_games"]
        )
        pokemon_df.reset_index(drop=True, inplace=True)
        merge_df = pd.merge(
            pokemon_df, pokemon_appear_in_games_df, left_index=True, right_index=True
        )
        df_pokemon_in_req_games = merge_df.query("Appear_in_chosen_games == True")[
            ["name", "id", "base_experience", "weight", "height", "order"]
        ]
        print("The length of dataframe is " + str(len(df_pokemon_in_req_games)))
        df_pokemon_in_req_games.to_csv(
            "pokemon_data_delivery_folder/pokemon_in_req_games.csv", index=False
        )
        return

    def get_pokemon_slot_name(self):
        """gets the slot names for type 1 and type 2(if present) in each of the Pokemon types"""
        print("getting pokemon slot names")
        pokemon_df = self.get_pokemon_normalized_df()
        pokemon_types_list = pokemon_df.types.tolist()
        pokemon_types_df = pd.DataFrame(pokemon_types_list, columns=["slot1", "slot2"])
        pokemon_types_data = json.loads(pokemon_types_df.to_json(orient="records"))
        pokemon_types_data_normalized = pd.json_normalize(
            pokemon_types_data, max_level=2
        )
        pokemon_slot_columns = pokemon_types_data_normalized[
            ["slot1.type.name", "slot2.type.name"]
        ].fillna("")
        pokemon_name_df = pd.DataFrame(
            pokemon_df["name"].values.tolist(), columns=["POKEMON_NAME"]
        )
        df_pokemon_slot_names = pd.merge(
            pokemon_name_df, pokemon_slot_columns, left_index=True, right_index=True
        )
        df_pokemon_slot_names.rename(
            columns={"slot1.type.name": "SLOT1_NAME", "slot2.type.name": "SLOT2_NAME"},
            inplace=True,
        )
        df_pokemon_slot_names.to_csv(
            "pokemon_data_delivery_folder/pokemon_slot_names.csv", index=False
        )
        return

    def get_pokemon_bmi(self):
        """gets the bmi for the Pokemons"""
        print("Getting BMI for Pokemon")
        pokemon_df = self.get_pokemon_normalized_df()
        pokemon_bmi = pokemon_df[["name", "weight", "height"]]
        pokemon_bmi["weight"] = pokemon_bmi["weight"] / 10
        pokemon_bmi["height"] = pokemon_bmi["height"] / 10
        pokemon_bmi["bmi"] = (pokemon_bmi["weight"] / pokemon_bmi["height"]).round(2)
        pokemon_bmi.rename(columns={"bmi": "bmi(kg/m)"}, inplace=True)
        pokemon_bmi.to_csv("pokemon_data_delivery_folder/pokemon_bmi.csv", index=False)
        return

    def capitalize_first_letter_pokemon_names(self):
        """Capitalizes the first letter for the pokemon"""
        print("Capitalizing first letter of the pokemon names")
        pokemon_df = self.get_pokemon_normalized_df()
        capitalized_name = pokemon_df["name"].str.capitalize()
        capitalized_name_df = pd.DataFrame(capitalized_name, columns=["name"])
        capitalized_name_df.to_csv(
            "pokemon_data_delivery_folder/capitalized.csv", index=False
        )
        return

    def get_url_front_default_sprite(self):
        """gets the url of the front default sprite"""
        print("Get url of the front default sprite")
        pokemon_df = self.get_pokemon_normalized_df()
        front_default_sprites_url_df = pd.DataFrame(
            pokemon_df["sprites.front_default"].values.tolist(),
            columns=["front_default_url"],
        )
        front_default_sprites_url_df.to_csv(
            "pokemon_data_delivery_folder/front_default_sprite_url.csv", index=False
        )
        return


if __name__ == "__main__":
    pokemon_data = PokemonData()
    pokemon_data.get_pokemon_slot_name()
