import SwiftUI

struct RecipeDetailView: View {
    
    let backgroundGradient = LinearGradient(
        colors: [Color.gray, Color.white],
        startPoint: .top, endPoint: .bottomTrailing)
    
    var id: String
    
    @State var recipeObject: MealDetail?
    @State var fetching = false
    @State var error: Error?
    
    @EnvironmentObject var service: AcademicEatsService
    
    var body: some View {
        ScrollView {
            if fetching {
                ProgressView()
            } else if error != nil {
                Text("Something wrong :(")
            } else if let recipeObject {
                //Can copy the async Image from RecipeCard over to here without the overlay
                VStack(spacing: 30) {
                    AsyncImage(url:URL(string: recipeObject.strMealThumb)) { image in
                        image
                            .resizable()
                            .scaledToFit()
                            .foregroundColor(.white.opacity(0.7))
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                            .aspectRatio(contentMode: .fill)
                    } placeholder: {
                        Image("Penne-Arrabiata-wide-FS")
                            .resizable()
                            .scaledToFit()
                            .foregroundColor(.white.opacity(0.7))
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                            .aspectRatio(contentMode: .fill)
                    }
                    // title
                    HStack {
                        Text(recipeObject.strMeal)
                            .font(.largeTitle)
                            .bold()
                            .multilineTextAlignment(.center)
                    }
                    
                    Divider()
                    // ingredients
                    Text("Ingredients")
                        .font(.largeTitle)
                        .bold()
                        .multilineTextAlignment(.center)
                    ForEach(recipeObject.ingredients, id: \.self.name) { ingredient in
                        HStack {
                            Text(ingredient.name)
                            Text(ingredient.measure)
                        }
                    }
                    Divider()
                    // instructions
                    Text("Instructions")
                        .font(.largeTitle)
                        .bold()
                        .multilineTextAlignment(.center)
                        
                    Text(recipeObject.strInstructions)
                        .padding(.init(top: 0, leading: 10, bottom: 0, trailing: 10))
                    Divider()
                }
                .animation(.easeIn(duration: 0.25))
                .background(backgroundGradient)
                .ignoresSafeArea(edges: .top)
            }
        }
        .ignoresSafeArea()
        .task(id: id) {
            await fetchRecipe(id: id)
        }
    }
    
    func fetchRecipe(id: String) async {
        print("ID > ", id)
        fetching = true
        do {
            let result = try await getRecipe(id: id).meals[0]
            recipeObject = result
            fetching = false
        } catch {
            print(error)
            fetching = false
        }
    }
}

struct RecipeDetailView_Previews: PreviewProvider {
    static var previews: some View {
        RecipeDetailView(id: "52772")
            .environmentObject(AcademicEatsService())
    }
}
