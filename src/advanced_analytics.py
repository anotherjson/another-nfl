"""
Advanced Analytics and Machine Learning Module

This module provides advanced statistical analysis and machine learning capabilities
for NFL data, including predictive modeling and pattern recognition.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Union
import logging
from pathlib import Path
import joblib
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
import duckdb
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NFLAnalytics:
    """Advanced analytics engine for NFL data"""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb"):
        """Initialize analytics engine with database connection"""
        self.db_path = db_path
        self.conn = None
        self.models = {}
        self.scalers = {}
        self.encoders = {}
        self._connect_db()
    
    def _connect_db(self) -> None:
        """Connect to DuckDB database"""
        try:
            self.conn = duckdb.connect(self.db_path, read_only=True)
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def load_player_data(self, season: int = 2023) -> pd.DataFrame:
        """Load comprehensive player data for analysis"""
        query = """
        SELECT 
            player_name,
            position,
            team,
            week,
            season,
            fantasy_points_ppr,
            fantasy_points_standard,
            targets,
            receptions,
            receiving_yards,
            receiving_touchdowns,
            rushing_attempts,
            rushing_yards,
            rushing_touchdowns,
            passing_attempts,
            passing_completions,
            passing_yards,
            passing_touchdowns,
            passing_interceptions
        FROM int_player_weekly_stats
        WHERE season = ? AND fantasy_points_ppr > 0
        ORDER BY player_name, week
        """
        
        try:
            df = self.conn.execute(query, [season]).df()
            logger.info(f"Loaded {len(df)} player records for season {season}")
            return df
        except Exception as e:
            logger.error(f"Failed to load player data: {e}")
            return pd.DataFrame()
    
    def load_team_data(self, season: int = 2023) -> pd.DataFrame:
        """Load team performance data"""
        query = """
        SELECT 
            team,
            week,
            season,
            total_epa,
            pass_epa,
            rush_epa,
            wins,
            losses
        FROM mart_weekly_team_stats
        WHERE season = ?
        ORDER BY team, week
        """
        
        try:
            df = self.conn.execute(query, [season]).df()
            logger.info(f"Loaded {len(df)} team records for season {season}")
            return df
        except Exception as e:
            logger.error(f"Failed to load team data: {e}")
            return pd.DataFrame()
    
    def create_fantasy_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create advanced features for fantasy modeling"""
        logger.info("Creating fantasy prediction features...")
        
        # Sort by player and week
        df = df.sort_values(['player_name', 'week'])
        
        # Calculate rolling averages
        rolling_windows = [3, 5, 8]
        
        for window in rolling_windows:
            df[f'fantasy_avg_{window}w'] = df.groupby('player_name')['fantasy_points_ppr'].transform(
                lambda x: x.rolling(window, min_periods=1).mean().shift(1)
            )
            df[f'targets_avg_{window}w'] = df.groupby('player_name')['targets'].transform(
                lambda x: x.rolling(window, min_periods=1).mean().shift(1)
            )
            df[f'touches_avg_{window}w'] = df.groupby('player_name')['rushing_attempts'].transform(
                lambda x: (x + df.loc[x.index, 'targets']).rolling(window, min_periods=1).mean().shift(1)
            )
        
        # Calculate season-to-date averages
        df['fantasy_std_season'] = df.groupby('player_name')['fantasy_points_ppr'].transform(
            lambda x: x.expanding().std().shift(1)
        )
        df['games_played'] = df.groupby('player_name').cumcount() + 1
        
        # Target share (approximate)
        team_targets = df.groupby(['team', 'week'])['targets'].sum().reset_index()
        team_targets.columns = ['team', 'week', 'team_targets']
        df = df.merge(team_targets, on=['team', 'week'], how='left')
        df['target_share'] = df['targets'] / df['team_targets'].replace(0, np.nan)
        
        # Position-specific features
        df['receiving_efficiency'] = df['receiving_yards'] / df['targets'].replace(0, np.nan)
        df['rushing_efficiency'] = df['rushing_yards'] / df['rushing_attempts'].replace(0, np.nan)
        df['td_rate'] = (df['receiving_touchdowns'] + df['rushing_touchdowns']) / (df['targets'] + df['rushing_attempts']).replace(0, np.nan)
        
        # Fill NaN values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        logger.info(f"Created features for {len(df)} records")
        return df
    
    def train_fantasy_model(self, df: pd.DataFrame, position: str = 'all') -> Dict:
        """Train fantasy points prediction model"""
        logger.info(f"Training fantasy model for position: {position}")
        
        # Filter by position if specified
        if position != 'all':
            df = df[df['position'] == position]
        
        if len(df) < 100:
            logger.warning(f"Insufficient data for position {position}: {len(df)} records")
            return {}
        
        # Define features
        feature_cols = [
            'fantasy_avg_3w', 'fantasy_avg_5w', 'fantasy_avg_8w',
            'targets_avg_3w', 'targets_avg_5w', 'targets_avg_8w',
            'touches_avg_3w', 'touches_avg_5w', 'touches_avg_8w',
            'fantasy_std_season', 'games_played', 'target_share',
            'receiving_efficiency', 'rushing_efficiency', 'td_rate'
        ]
        
        # Prepare data
        X = df[feature_cols].copy()
        y = df['fantasy_points_ppr'].copy()
        
        # Encode categorical variables if needed
        if position == 'all':
            le = LabelEncoder()
            X['position_encoded'] = le.fit_transform(df['position'])
            feature_cols.append('position_encoded')
            self.encoders[f'{position}_position'] = le
        
        # Remove rows with NaN target values
        valid_mask = ~y.isna()
        X = X[valid_mask]
        y = y[valid_mask]
        
        if len(X) < 50:
            logger.warning(f"Insufficient valid data for position {position}: {len(X)} records")
            return {}
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=df[valid_mask]['week'] if len(df[valid_mask]) > 100 else None
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train multiple models
        models = {
            'random_forest': RandomForestRegressor(n_estimators=100, random_state=42),
            'gradient_boosting': GradientBoostingRegressor(n_estimators=100, random_state=42),
            'linear_regression': LinearRegression(),
            'ridge': Ridge(alpha=1.0)
        }
        
        results = {}
        best_model = None
        best_score = -np.inf
        
        for name, model in models.items():
            try:
                # Train model
                if name in ['linear_regression', 'ridge']:
                    model.fit(X_train_scaled, y_train)
                    y_pred = model.predict(X_test_scaled)
                else:
                    model.fit(X_train, y_train)
                    y_pred = model.predict(X_test)
                
                # Calculate metrics
                mae = mean_absolute_error(y_test, y_pred)
                rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                r2 = r2_score(y_test, y_pred)
                
                results[name] = {
                    'model': model,
                    'mae': mae,
                    'rmse': rmse,
                    'r2': r2,
                    'predictions': y_pred
                }
                
                if r2 > best_score:
                    best_score = r2
                    best_model = name
                
                logger.info(f"{name} - MAE: {mae:.2f}, RMSE: {rmse:.2f}, R2: {r2:.3f}")
                
            except Exception as e:
                logger.error(f"Failed to train {name}: {e}")
        
        # Store best model and scaler
        if best_model:
            self.models[position] = results[best_model]['model']
            self.scalers[position] = scaler
            
            model_info = {
                'best_model': best_model,
                'best_score': best_score,
                'results': results,
                'feature_cols': feature_cols,
                'scaler': scaler
            }
            
            logger.info(f"Best model for {position}: {best_model} (R2: {best_score:.3f})")
            return model_info
        
        return {}
    
    def predict_fantasy_points(self, df: pd.DataFrame, position: str = 'all') -> pd.DataFrame:
        """Make fantasy points predictions"""
        if position not in self.models:
            logger.error(f"No trained model found for position: {position}")
            return df
        
        model = self.models[position]
        scaler = self.scalers[position]
        
        # Use same features as training
        feature_cols = [
            'fantasy_avg_3w', 'fantasy_avg_5w', 'fantasy_avg_8w',
            'targets_avg_3w', 'targets_avg_5w', 'targets_avg_8w',
            'touches_avg_3w', 'touches_avg_5w', 'touches_avg_8w',
            'fantasy_std_season', 'games_played', 'target_share',
            'receiving_efficiency', 'rushing_efficiency', 'td_rate'
        ]
        
        X = df[feature_cols].fillna(0)
        
        # Add position encoding if needed
        if position == 'all' and f'{position}_position' in self.encoders:
            X['position_encoded'] = self.encoders[f'{position}_position'].transform(df['position'])
        
        # Make predictions
        try:
            if isinstance(model, (LinearRegression, Ridge)):
                X_scaled = scaler.transform(X)
                predictions = model.predict(X_scaled)
            else:
                predictions = model.predict(X)
            
            df['predicted_fantasy_points'] = predictions
            df['prediction_confidence'] = np.minimum(1.0, np.maximum(0.0, 
                1.0 - (df['fantasy_std_season'] / df['fantasy_avg_5w'].replace(0, np.nan)).fillna(0.5)
            ))
            
            logger.info(f"Generated predictions for {len(df)} records")
            
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            df['predicted_fantasy_points'] = 0
            df['prediction_confidence'] = 0
        
        return df
    
    def analyze_player_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze player consistency and reliability"""
        logger.info("Analyzing player consistency...")
        
        consistency_stats = df.groupby(['player_name', 'position', 'team']).agg({
            'fantasy_points_ppr': ['mean', 'std', 'min', 'max', 'count'],
            'targets': 'sum',
            'rushing_attempts': 'sum'
        }).round(2)
        
        # Flatten column names
        consistency_stats.columns = ['avg_points', 'std_points', 'min_points', 'max_points', 'games', 'total_targets', 'total_carries']
        consistency_stats = consistency_stats.reset_index()
        
        # Calculate consistency metrics
        consistency_stats['consistency_score'] = consistency_stats['avg_points'] / (consistency_stats['std_points'] + 0.1)
        consistency_stats['floor'] = consistency_stats['avg_points'] - consistency_stats['std_points']
        consistency_stats['ceiling'] = consistency_stats['avg_points'] + consistency_stats['std_points']
        consistency_stats['usage_score'] = consistency_stats['total_targets'] + consistency_stats['total_carries']
        
        # Filter for relevant players
        consistency_stats = consistency_stats[consistency_stats['games'] >= 8]
        
        logger.info(f"Analyzed consistency for {len(consistency_stats)} players")
        return consistency_stats
    
    def team_strength_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze team strength and EPA trends"""
        logger.info("Analyzing team strength...")
        
        team_stats = df.groupby('team').agg({
            'total_epa': ['mean', 'std', 'min', 'max'],
            'pass_epa': 'mean',
            'rush_epa': 'mean',
            'wins': 'sum',
            'losses': 'sum'
        }).round(3)
        
        # Flatten column names
        team_stats.columns = ['avg_epa', 'epa_std', 'min_epa', 'max_epa', 'avg_pass_epa', 'avg_rush_epa', 'total_wins', 'total_losses']
        team_stats = team_stats.reset_index()
        
        # Calculate additional metrics
        team_stats['win_rate'] = team_stats['total_wins'] / (team_stats['total_wins'] + team_stats['total_losses'])
        team_stats['consistency'] = team_stats['avg_epa'] / (team_stats['epa_std'] + 0.01)
        team_stats['offensive_balance'] = abs(team_stats['avg_pass_epa'] - team_stats['avg_rush_epa'])
        
        # Rank teams
        team_stats['epa_rank'] = team_stats['avg_epa'].rank(ascending=False)
        team_stats['consistency_rank'] = team_stats['consistency'].rank(ascending=False)
        
        logger.info(f"Analyzed {len(team_stats)} teams")
        return team_stats
    
    def save_models(self, model_dir: str = "models") -> None:
        """Save trained models to disk"""
        model_path = Path(model_dir)
        model_path.mkdir(exist_ok=True)
        
        for position, model in self.models.items():
            model_file = model_path / f"fantasy_model_{position}.joblib"
            scaler_file = model_path / f"scaler_{position}.joblib"
            
            joblib.dump(model, model_file)
            joblib.dump(self.scalers.get(position), scaler_file)
            
            logger.info(f"Saved model for {position}")
    
    def load_models(self, model_dir: str = "models") -> None:
        """Load trained models from disk"""
        model_path = Path(model_dir)
        
        if not model_path.exists():
            logger.warning(f"Model directory {model_dir} does not exist")
            return
        
        for model_file in model_path.glob("fantasy_model_*.joblib"):
            position = model_file.stem.replace("fantasy_model_", "")
            scaler_file = model_path / f"scaler_{position}.joblib"
            
            try:
                self.models[position] = joblib.load(model_file)
                if scaler_file.exists():
                    self.scalers[position] = joblib.load(scaler_file)
                
                logger.info(f"Loaded model for {position}")
                
            except Exception as e:
                logger.error(f"Failed to load model for {position}: {e}")
    
    def generate_insights_report(self) -> Dict:
        """Generate comprehensive analytics insights"""
        logger.info("Generating insights report...")
        
        try:
            # Load data
            player_df = self.load_player_data()
            team_df = self.load_team_data()
            
            if player_df.empty or team_df.empty:
                logger.warning("No data available for insights")
                return {}
            
            # Create features
            player_df = self.create_fantasy_features(player_df)
            
            # Analyze consistency
            consistency = self.analyze_player_consistency(player_df)
            
            # Analyze team strength
            team_strength = self.team_strength_analysis(team_df)
            
            # Top insights
            insights = {
                'top_consistent_players': consistency.nlargest(10, 'consistency_score')[
                    ['player_name', 'position', 'team', 'avg_points', 'consistency_score']
                ].to_dict('records'),
                
                'highest_ceiling_players': consistency.nlargest(10, 'max_points')[
                    ['player_name', 'position', 'team', 'max_points', 'avg_points']
                ].to_dict('records'),
                
                'strongest_teams': team_strength.nlargest(10, 'avg_epa')[
                    ['team', 'avg_epa', 'win_rate', 'avg_pass_epa', 'avg_rush_epa']
                ].to_dict('records'),
                
                'most_consistent_teams': team_strength.nlargest(10, 'consistency')[
                    ['team', 'consistency', 'avg_epa', 'epa_std']
                ].to_dict('records'),
                
                'summary_stats': {
                    'total_players': len(consistency),
                    'total_teams': len(team_strength),
                    'avg_fantasy_points': player_df['fantasy_points_ppr'].mean(),
                    'avg_team_epa': team_df['total_epa'].mean()
                }
            }
            
            logger.info("Generated comprehensive insights report")
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main function for testing advanced analytics"""
    analytics = NFLAnalytics()
    
    try:
        logger.info("=== NFL Advanced Analytics Demo ===")
        
        # Load data
        player_data = analytics.load_player_data()
        logger.info(f"Loaded {len(player_data)} player records")
        
        # Create features
        player_data = analytics.create_fantasy_features(player_data)
        
        # Train models for different positions
        positions = ['QB', 'RB', 'WR', 'TE']
        
        for position in positions:
            logger.info(f"\n=== Training model for {position} ===")
            model_info = analytics.train_fantasy_model(player_data, position)
            
            if model_info:
                logger.info(f"Best model: {model_info['best_model']}")
                logger.info(f"R² Score: {model_info['best_score']:.3f}")
        
        # Generate insights
        logger.info("\n=== Generating Insights Report ===")
        insights = analytics.generate_insights_report()
        
        if insights:
            logger.info("\n=== TOP CONSISTENT PLAYERS ===")
            for player in insights['top_consistent_players'][:5]:
                logger.info(f"{player['player_name']} ({player['position']}): {player['consistency_score']:.2f}")
            
            logger.info("\n=== STRONGEST TEAMS ===")
            for team in insights['strongest_teams'][:5]:
                logger.info(f"{team['team']}: EPA {team['avg_epa']:.3f}, Win Rate {team['win_rate']:.1%}")
        
        # Save models
        analytics.save_models()
        logger.info("\n=== Models saved successfully ===")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
    
    finally:
        analytics.close()

if __name__ == "__main__":
    main()