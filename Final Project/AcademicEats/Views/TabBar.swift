import SwiftUI

struct TabBar: View {
    @EnvironmentObject var auth: AcademicEatsAuth
    @EnvironmentObject var service: AcademicEatsService
    @Binding var requestLogin: Bool
    
    @State private var selectedTab: Tabs = .home
    @State var isNavigationBarHidden: Bool = false
    @State var tabSelection = 0
    
    enum Tabs: String{
        case home
        case search
        case favorite
    }
    
    var body: some View {
        NavigationView {
            TabView(selection: $selectedTab) {
                
                SearchRecipe(isNavigationBarHidden: $isNavigationBarHidden)
                    .tabItem {
                        Label("Search", systemImage: "sparkle.magnifyingglass")
                    }
                    .tag(Tabs.search)
                    .animation(.spring(), value: tabSelection)
                HomeView(isNavigationBarHidden: $isNavigationBarHidden)
                    .tabItem {
                        Label("Home", systemImage: "house.circle")
                    }
                    .tag(Tabs.home)
                    .animation(.spring(), value: tabSelection)
                
                FavoriteRecipes(isNavigationBarHidden: $isNavigationBarHidden)
                    .tabItem {
                        Label("Favorite Meals", systemImage: "cooktop.fill")
                    }
                    .tag(Tabs.favorite)
                    .animation(.spring(), value: tabSelection)
            }
            .navigationTitle(selectedTab != .search ? selectedTab.rawValue.capitalized : "")
            .navigationBarHidden(self.isNavigationBarHidden)
            .toolbar {
                ToolbarItemGroup(placement: .navigationBarTrailing) {
                    if auth.user != nil {
                        Button("Sign Out") {
                            do {
                                try auth.signOut()
                            } catch {
                                // error catch needed
                            }
                        }
                    } else {
                        Button("Sign In") {
                            requestLogin = true
                        }
                    }
                }
            }
            .onAppear {
                self.isNavigationBarHidden = false
            }
        }
    }
}

struct TabBar_Previews: PreviewProvider {
    @State static var requestLogin = false
    
    static var previews: some View {
        TabBar(requestLogin: $requestLogin)
            .environmentObject(AcademicEatsAuth())
            .environmentObject(AcademicEatsService())
    }
}
