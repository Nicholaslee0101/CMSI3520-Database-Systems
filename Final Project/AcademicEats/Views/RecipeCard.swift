import SwiftUI

struct RecipeCard: View {
    @State var result: SubMealWithFavorite
    
    @EnvironmentObject var auth: AcademicEatsAuth
    var body: some View {
        VStack {
            
            AsyncImage(url: URL(string: result.meal.strMealThumb)) { image in
                image
                    .resizable()
                    .aspectRatio(contentMode: .fill)
                    .overlay(alignment: .bottom) {
                        Text(result.meal.strMeal)
                            .font(.headline)
                            .foregroundColor(.white)
                            .shadow(color: .black, radius: 3, x:0, y: 0)
                            .frame(maxWidth: 136)
                            .padding()
                    }
                    .overlay(alignment: .top) {
                        if auth.user != nil {
                            FavoriteButton(isSet: $result.favorite, meal: result.meal)
                        }
                    }
                
            } placeholder: {
                Image("Penne-Arrabiata-wide-FS")
                    .resizable()
                    .scaledToFit()
                    .foregroundColor(.white.opacity(0.7))
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .aspectRatio(contentMode: .fill)
                    .overlay(alignment: .bottom) {
                        Text("Loading")
                            .font(.headline)
                            .foregroundColor(.white)
                            .shadow(color: .black, radius: 3, x: 0, y: 0)
                            .frame(maxWidth: 136)
                            .padding()
                    }
            }
        }
        
        //This code should be outside of the Async Imag
        .frame(width: 160, height: 217, alignment: .top)
        .background(LinearGradient(gradient: Gradient(colors: [Color(.gray).opacity(0.3), Color(.gray)]), startPoint: .top, endPoint: .bottom))
        .clipShape(RoundedRectangle(cornerRadius: 20,style: .continuous))
        .shadow(color: Color.black.opacity(0.3), radius: 15, x:0, y:10)
    }
}

struct RecipeCard_Previews: PreviewProvider {
    static var previews: some View {
        RecipeCard(result: SubMealWithFavorite(meal: SubMeal(strMeal: "Preview", strMealThumb: "Penne-Arrabiata-wide-FS", idMeal: "5")))
            .environmentObject(AcademicEatsAuth())
    }
}
