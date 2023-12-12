import Foundation
import SwiftUI


struct MealList: Codable, Hashable {
    var meals: [SubMeal]?
}

struct SubMeal: Codable, Hashable, Identifiable{
    var id: String {
        idMeal
    }
    var strMeal: String
    var strMealThumb: String
    var idMeal: String
}

struct SubMealWithFavorite {
    var meal: SubMeal
    var favorite: Bool = false
}
