import SwiftUI

struct FavoriteRecipes: View {
    @EnvironmentObject var auth: AcademicEatsAuth
    @EnvironmentObject var service: AcademicEatsService
    
    @State var error: Error?
    @State var fetching = false
    @State var result:[SubMealWithFavorite] = []
    
    @Binding var isNavigationBarHidden: Bool
    
    func loadFavoriteMeals() async {
        if auth.user != nil {
            result = []
            fetching = true
            
            do {
                result = try await service.fetchFavoriteMeals(collectionName: auth.user!.uid)
            } catch {
                print(error)
                self.error = error
            }
            fetching = false
        }
    }
    
    var body: some View {
        ZStack {
            Circle()
                .scale(1.65)
                .foregroundColor(.blue)
            Circle()
                .scale(1.5)
                .foregroundColor(.white.opacity(0.15))
            Circle()
                .scale(1.15)
                .foregroundColor(.white)
            VStack {
                if fetching {
                    ProgressView()
                }
                else if error != nil {
                    Text("Something went wrong…we wish we can say more 🤷🏽")
                } else {
                    if auth.user != nil {
                        RecipeList(results: result)
                    } else {
                        Image(systemName: "person.circle").font(.system(size: 100)).foregroundColor(Color.blue)
                        Text("What to see your favorite foods?")
                        Text("Please sign in above").foregroundColor(Color.blue).padding(5)
                    }
                }
            }
            .onChange(of: service.favoriteMeals) { newValue in
                Task {
                    await loadFavoriteMeals()
                }
            }
            .refreshable {
                Task {
                    await loadFavoriteMeals()
                }
            }
        }
        .onAppear {
            isNavigationBarHidden = false
        }
    }
}

struct FavoriteRecipes_Previews: PreviewProvider {
    static var previews: some View {
        FavoriteRecipes(isNavigationBarHidden: .constant(false))
            .environmentObject(AcademicEatsAuth())
            .environmentObject(AcademicEatsService())
    }
}
