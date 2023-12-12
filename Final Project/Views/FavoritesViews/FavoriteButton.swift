import SwiftUI

struct FavoriteButton: View {
    @Binding var isSet: Bool
    @State var meal: SubMeal
    
    @EnvironmentObject var auth: AcademicEatsAuth
    @EnvironmentObject var service: AcademicEatsService

    var animation: Animation {
        Animation.easeOut
    }

    var body: some View {
        Button(action: {
            isSet.toggle()
            if isSet {
                service.saveFavoriteMeals(meal: meal, collectionName: auth.user!.uid)
                service.favoriteMeals.append(meal.id)
            } else {
                service.favoriteMeals = service.favoriteMeals.filter {$0 != meal.id}
                service.removeFavorite = true
                service.deleteFavoriteMeal(collectionName: auth.user!.uid, id: meal.id)
            }
        }) {
            Image(systemName: isSet ? "star.fill" : "star")
                .foregroundColor(isSet ? .yellow : .white)
                .rotationEffect(Angle.degrees(isSet ? 360: 0))
        }
    }
}

struct FavoriteButton_Previews: PreviewProvider {
    static var previews: some View {
        FavoriteButton(isSet: .constant(true), meal: SubMeal(strMeal: "Preview", strMealThumb: "Penne-Arrabiata-wide-FS", idMeal: "5"))
            .environmentObject(AcademicEatsService())
            .environmentObject(AcademicEatsAuth())
    }
}
