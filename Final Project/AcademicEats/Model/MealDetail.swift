import Foundation

struct MealDetailData: Codable, Hashable {
    var meals : [MealDetail]
}

struct MealDetail: Codable, Hashable, Identifiable {
    var id : String {
        idMeal
    }
    var idMeal : String
    var strMeal : String
    var strCategory: String
    var strArea: String
    var strInstructions: String
    var strMealThumb: String
    var strYoutube: String
    var strIngredient1: String?
    var strIngredient2: String?
    var strIngredient3: String?
    var strIngredient4: String?
    var strIngredient5: String?
    var strIngredient6: String?
    var strIngredient7: String?
    var strIngredient8: String?
    var strIngredient9: String?
    var strIngredient10: String?
    var strIngredient11: String?
    var strIngredient12: String?
    var strIngredient13: String?
    var strIngredient14: String?
    var strIngredient15: String?
    var strIngredient16: String?
    var strIngredient17: String?
    var strIngredient18: String?
    var strIngredient19: String?
    var strIngredient20: String?
    var strMeasure1: String?
    var strMeasure2: String?
    var strMeasure3: String?
    var strMeasure4: String?
    var strMeasure5: String?
    var strMeasure6: String?
    var strMeasure7: String?
    var strMeasure8: String?
    var strMeasure9: String?
    var strMeasure10: String?
    var strMeasure11: String?
    var strMeasure12: String?
    var strMeasure13: String?
    var strMeasure14: String?
    var strMeasure15: String?
    var strMeasure16: String?
    var strMeasure17: String?
    var strMeasure18: String?
    var strMeasure19: String?
    var strMeasure20: String?
    // organizes the data better (especially for retrieval!)
    var ingredients: [(name: String, measure: String)] {
        return [(strIngredient1, strMeasure1),
                (strIngredient2, strMeasure2),
                (strIngredient3, strMeasure3),
                (strIngredient4, strMeasure4),
                (strIngredient5, strMeasure5),
                (strIngredient6, strMeasure6),
                (strIngredient7, strMeasure7),
                (strIngredient8, strMeasure8),
                (strIngredient9, strMeasure9),
                (strIngredient10, strMeasure10),
                (strIngredient11, strMeasure11),
                (strIngredient12, strMeasure12),
                (strIngredient13, strMeasure13),
                (strIngredient14, strMeasure14),
                (strIngredient15, strMeasure15),
                (strIngredient16, strMeasure16),
                (strIngredient17, strMeasure17),
                (strIngredient18, strMeasure18),
                (strIngredient19, strMeasure19),
                (strIngredient20, strMeasure20)].compactMap {
            if let ingredient = $0.0, let measure = $0.1, !ingredient.isEmpty && !measure.isEmpty {
                return (ingredient, measure)
            }
            return nil
        }
    }
}
