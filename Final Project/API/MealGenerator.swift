import Foundation

enum MealError: Error {
    case unsucessfulDecode
    case MealsNotFound
    case unexpectedAPIError
    case invalidApiKey
}

let MealDB_ROOT = "https://www.themealdb.com/api/json/v1/1/"

func getMealsByCategory(category: String) async throws -> MealList {
    guard let url = URL(string: "\(MealDB_ROOT)filter.php?c=\(category)")
    else {
        fatalError("ERROR: Incorrect Error")
    }
    //debug for RecipeCarousel
//    print(url)
    let (data, _) = try await URLSession.shared.data(from: url)
    //debug for why not decoding
//    print(data)
    if let decodedPage = try?
        JSONDecoder().decode(MealList.self, from : data){
        return decodedPage
    } else {
        throw MealError.unsucessfulDecode
    }
}

func getMealsByIngredient(ingredients: String) async throws -> MealList {
    guard let url = URL(string: "\(MealDB_ROOT)filter.php?i=\(ingredients)") else { fatalError("ERROR: Incorrect URL")
    }
    
    let (data, _) = try await URLSession.shared.data(from: url)
    if let decodedPage = try?
        JSONDecoder().decode(MealList.self, from : data){
        return decodedPage
    } else {
        throw MealError.unsucessfulDecode
    }
}

func getRecipe(id: String) async throws -> MealDetailData {
    guard let url = URL(string: "https://www.themealdb.com/api/json/v1/1/lookup.php?i=\(id)") else{
        fatalError("Error: Incorrect URL")
    }
    let(data, _) = try await URLSession.shared.data(from: url)
    if let decodedPage = try?
        JSONDecoder().decode(MealDetailData.self, from: data) {
        return decodedPage
    } else {
        throw MealError.unsucessfulDecode
    }
}
