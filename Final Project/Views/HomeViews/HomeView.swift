import SwiftUI

struct HomeView: View {
    @State var categories: [String] = ["Beef", "Breakfast", "Chicken", "Dessert", "Goat", "Lamb", "Miscellaneous", "Pasta", "Pork", "Seafood", "Side", "Starter", "Vegan"]
    
    @EnvironmentObject var service: AcademicEatsService
    
    @Binding var isNavigationBarHidden: Bool
    
    var body: some View {
        ScrollView {
            ForEach(categories, id: \.self) { category in
                RecipeCarousel(category: category)
            }
        }
        .navigationTitle("Home")
        .onAppear {
            self.isNavigationBarHidden = false
        }
    }
}

struct HomeView_Previews: PreviewProvider {
    static var previews: some View {
        HomeView(isNavigationBarHidden: .constant(false))
            .environmentObject(AcademicEatsService())
    }
}
