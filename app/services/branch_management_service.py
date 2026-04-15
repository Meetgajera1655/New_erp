from app.repositories.branch_repo import BranchRepository

class BranchManagementService:

    @staticmethod
    def get_dashboard_kpi(db, schema, **kwargs):
        return {
            "active_branches": BranchRepository.count_active_branches(db, schema, **kwargs),

            "branch_type_distribution": [
                {"branch_type": r[0], "count": r[1]}
                for r in BranchRepository.branch_type_distribution(db, schema, **kwargs)
            ],

            "branch_density": [
                {"state": r[0], "count": r[1]}
                for r in BranchRepository.branch_density(db, schema, **kwargs)
            ],

            "employee_branch_ratio": round(
                BranchRepository.employee_branch_ratio(db, schema, **kwargs) or 0, 2
            )
        }
    

    @staticmethod
    def get_branch_charts(db, schema, **kwargs):
        return {
        "branch_growth_trend": [
            {"month": r[0], "count": r[1]}
            for r in BranchRepository.branch_growth_trend(db, schema, **kwargs)
        ],
        "state_distribution": [
            {"state": r[0], "count": r[1]}
            for r in BranchRepository.branch_density(db, schema, **kwargs)
        ],
        "status_breakdown": [
            {"status": r[0], "count": r[1]}
            for r in BranchRepository.status_breakdown(db, schema, **kwargs)
        ]
    }

    @staticmethod
    def get_branch_tables(db, schema, **kwargs):
        return {
        "branch_directory": [
            {
                "branch_name": r[0],
                "branch_code": r[1],
                "city": r[2],
                "email": r[3],
                "phone_number": r[4]
            }
            for r in BranchRepository.branch_directory(db, schema, **kwargs)
        ],

        "recent_branch_activations": [
            {
                "branch_name": r[0],
                "branch_code": r[1],
                "branch_type": r[2],
                "created_at": str(r[3]),
                "created_by": r[4]
            }
            for r in BranchRepository.recent_branch_activations(db, schema, **kwargs)
        ]
    }

    @staticmethod
    def get_branch_alerts(db, schema, **kwargs):
        return {
        "inactive_branch_alerts": [
            {
                "branch_name": r[0],
                "branch_code": r[1],
                "status": r[2]
            }
            for r in BranchRepository.inactive_branches(db, schema, **kwargs)
        ],

        "invalid_branch_code_alerts": [
            {
                "branch_name": r[0],
                "branch_code": r[1]
            }
            for r in BranchRepository.invalid_branch_codes(db, schema, **kwargs)
        ]
    }