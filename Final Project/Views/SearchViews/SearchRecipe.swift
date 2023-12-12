import SwiftUI

struct SearchRecipe: View {
    @State var results: [SubMealWithFavorite] = []
    @State var showLists = false
    
    @State var loading = false
    @State var potentialError: Error?
    @State private var searchText: String = ""
    @State private var isRotated = false
    
    @Binding var isNavigationBarHidden: Bool
    
    @EnvironmentObject var service: AcademicEatsService
    
    var animation: Animation {
        Animation.linear
            .repeatForever(autoreverses: false)
    }
    
    var body: some View {
        NavigationStack {
            ZStack {
                VStack(alignment: .leading) {
                    if loading {
                        ProgressView()
                    } else if potentialError != nil {
                        Text("Sorry, something went wrong: data could not be parsed")
                    } else {
                        if showLists {
                            if results.count == 0 {
                                VStack {
                                    Image(systemName: "magnifyingglass.circle").font(.system(size: 100)).foregroundColor(Color.blue).padding(10)
                                        .rotationEffect(Angle.degrees(isRotated ? 360 : 0))
                                        .animation(animation, value: self.isRotated)
                                    
                                    Text("Please search ingredent (Ex: Chicken)").foregroundColor(Color.blue)
                                }
                                .onAppear {
                                    self.isRotated.toggle()
                                }
                                
                            } else {
                                RecipeList(results: results)
                            }
                        }
                    }
                }
                .onAppear {
                    self.isNavigationBarHidden = true
                }
                .navigationTitle("Search")
                .searchable(text: $searchText)
                // When user is typing, the RecipeList would not show
                .onChange(of: searchText) { newValue in
                    showLists = false
                }
                .onChange(of: service.favoriteMeals) { newValue in
                    Task {
                        await loadSearchResult()
                    }
                }
                // When user hit Done or Return key, fetch API
                .onSubmit(of: .search) {
                    Task {
                        await loadSearchResult()
                        showLists = true
                    }
                }
                .refreshable {
                    Task {
                        await loadSearchResult()
                        showLists = true
                    }
                }
            }
        }
    }
    
    func loadSearchResult() async {
        results = []
        var jsonData: [SubMeal] = []
        loading = true
        
        do {
            print(searchText)
            let searchPage = try await getMealsByIngredient(ingredients: searchText.lowercased().replacingOccurrences(of: " ", with: "_"))
            // If meals is nil, then results would be an empty list
            jsonData = searchPage.meals ?? []
            
            for data in jsonData {
                if (service.favoriteMeals.contains(data.id)) {
                    results.append(SubMealWithFavorite(meal: data, favorite: true))
                } else {
                    results.append(SubMealWithFavorite(meal: data))
                }
            }
        } catch {
            potentialError = error
            debugPrint("Unexpected Error: \(error)")
        }
        loading = false
    }
}

struct SearchRecipe_Previews: PreviewProvider {
    static var previews: some View {
        SearchRecipe(isNavigationBarHidden: .constant(true))
            .environmentObject(AcademicEatsService())
    }
}
