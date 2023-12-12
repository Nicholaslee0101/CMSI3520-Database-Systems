import SwiftUI

struct RecipeList: View {
    @State var loading = false
    @State var potentialError: Error?
    @State var results: [SubMealWithFavorite]
    @State var recipe: MealDetail?
    
    @EnvironmentObject var service: AcademicEatsService
    
    var body: some View {
        ScrollView {
            VStack {
                if loading {
                    ProgressView()
                }
                else if potentialError != nil {
                    Text("Sorry something went wrong")
                }
                else {
                    HStack {
                        Text("\(results.count) \(results.count > 1 ? "recipes": "recipe")")
                            .font(.headline)
                            .fontWeight(.medium)
                            .opacity(0.7)
                        Spacer()
                    }
                    
                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 160), spacing: 15)]) {
                        // idMeal is a String that is the meal id that we can get a Meal Detail through getRecipe() that is async
                        ForEach(results, id: \.meal.idMeal) { meal in
                            
                            NavigationLink {
                                // needs a meal detail object
                                RecipeDetailView(id: meal.meal.idMeal)
                            } label: {
                                RecipeCard(result: meal)
                            }
                        }
                    }
                }
            }
        }
        .padding(.horizontal)
    }
}

struct RecipeList_Previews: PreviewProvider {
    static var previews: some View {
        RecipeList(results: [])
            .environmentObject(AcademicEatsService())
    }
}
