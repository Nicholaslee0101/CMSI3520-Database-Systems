import Foundation
import Firebase

let PAGE_LIMIT = 30

enum FirebaseServiceError: Error {
    case mismatchedDocumentError
    case unexpectedError
}

class AcademicEatsService: ObservableObject {
    private let db = Firestore.firestore()
    @Published var error: Error?
    
    @Published var favoriteMeals: [String] = []
    @Published var removeFavorite: Bool = false
    
    // Add favorite meals from
    func saveFavoriteMeals(meal: SubMeal, collectionName: String) {
        db.collection(collectionName).document(meal.id).setData([
            "strMeal": meal.strMeal,
            "strMealThumb": meal.strMealThumb,
            "idMeal": meal.idMeal
        ]) { err in
            if let err = err {
                print("Error writing document: \(err)")
            } else {
                print("Document successfully written!")
            }
        }
    }
    
    func fetchFavoriteMeals(collectionName: String) async throws -> [SubMealWithFavorite] {
        let favoriteMeals = db.collection(collectionName)
            .order(by: "strMeal", descending: true)
            .limit(to: PAGE_LIMIT)
        
        let querySnapshot = try await favoriteMeals.getDocuments()
        
        return try querySnapshot.documents.map {
            guard let strMeal = $0.get("strMeal") as? String,
                  let strMealThumb = $0.get("strMealThumb") as? String,
                  let idMeal = $0.get("idMeal") as? String else {
                throw FirebaseServiceError.mismatchedDocumentError
            }
            
            return SubMealWithFavorite(
                meal: SubMeal( strMeal: strMeal,
                               strMealThumb: strMealThumb,
                               idMeal: idMeal),
                favorite: true
            )
        }
    }
    
    func deleteFavoriteMeal(collectionName: String, id: String) {
        db.collection(collectionName).document(id).delete() { err in
            if let err = err {
                print("Error removing document: \(err)")
            } else {
                print("Document successfully removed!")
            }
        }
    }
}
