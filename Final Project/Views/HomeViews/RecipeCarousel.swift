import SwiftUI

struct RecipeCarousel: View {
    @State var fetching = false
    // what we pass into to build the carousel
    @State var category: String
    // list to generate our horizontal scroll
    @State var subMeals: [SubMealWithFavorite]?
    
    var body: some View {
        VStack {
            Divider()
            Text(category)
                .foregroundStyle(.black.gradient)
                .font(.title)
                .fontDesign(.serif)
            if fetching {
                ProgressView()
            } else if let subMeals {
                ScrollView(.horizontal) {
                    HStack(spacing: 10) {
                        ForEach(subMeals, id: \.meal.idMeal) { meal in
                            NavigationLink {
                                RecipeDetailView(id: meal.meal.idMeal)
                            } label: {
                                RecipeCard(result: meal)
                            }
                        }
                    }.padding()
                }.frame(height: 232)
            }
            Divider()
            Spacer()
        }
        .navigationViewStyle(.stack)
        .task() {
            await fetchCategoryMeals(category: category)
        }
    }
    // only need to run the task once
    
    
    //function calls a SubMeal list for a given category
    func fetchCategoryMeals(category: String) async {
        // use the same fetching to make sure ProgressScreen for users
        fetching = true
        subMeals = []
        do {
            if let result = try await getMealsByCategory(category: category).meals {
                for data in result {
                    subMeals?.append(SubMealWithFavorite(meal: data))
                }
                fetching = false
            }
        } catch {
            print(error)
            fetching = false
        }
    }
}

struct RecipeCarousel_Previews: PreviewProvider {
    static var previews: some View {
        RecipeCarousel(category: "Vegan")
    }
}
